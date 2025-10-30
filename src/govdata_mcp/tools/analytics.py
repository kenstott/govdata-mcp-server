"""Analytics tools for exploratory data analysis using ML/statistics."""

from typing import Dict, Any, Optional, List
from ..jdbc import get_connection
import logging
import numpy as np
import pandas as pd
from scipy import stats
from sklearn.ensemble import IsolationForest
from sklearn.cluster import KMeans, DBSCAN
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score

logger = logging.getLogger(__name__)

# Safety limits to prevent OOM
MAX_ROWS_FOR_ML = 100000
DEFAULT_SAMPLE_SIZE = 20


def _fetch_data(sql: str, limit: Optional[int] = None) -> pd.DataFrame:
    """
    Helper: Fetch SQL results as pandas DataFrame.

    Args:
        sql: SQL query
        limit: Row limit (None = no limit, but MAX_ROWS_FOR_ML still enforced)

    Returns:
        pandas DataFrame

    Raises:
        ValueError: If result exceeds MAX_ROWS_FOR_ML
    """
    conn = get_connection()

    # Apply limit if specified
    if limit and "LIMIT" not in sql.upper():
        sql = f"{sql} LIMIT {limit}"

    columns, rows = conn.execute_query(sql)
    df = pd.DataFrame(rows, columns=columns)

    if len(df) == 0:
        raise ValueError("Query returned no data")

    if len(df) > MAX_ROWS_FOR_ML:
        raise ValueError(
            f"Query returned {len(df)} rows, which exceeds MAX_ROWS_FOR_ML "
            f"({MAX_ROWS_FOR_ML}). Please add a LIMIT clause to your SQL query."
        )

    logger.info(f"Fetched {len(df)} rows with {len(df.columns)} columns for analysis")
    return df


def _select_features(
    df: pd.DataFrame,
    features: Optional[List[str]] = None
) -> tuple[pd.DataFrame, List[str]]:
    """
    Helper: Select numeric columns for analysis.

    Args:
        df: Input DataFrame
        features: Specific column names (None = all numeric)

    Returns:
        Tuple of (selected DataFrame, list of feature names)

    Raises:
        ValueError: If no numeric columns found or specified columns don't exist
    """
    if features:
        # Validate specified columns exist
        missing = [f for f in features if f not in df.columns]
        if missing:
            raise ValueError(f"Columns not found in data: {missing}")

        # Validate they're numeric
        non_numeric = [f for f in features if not pd.api.types.is_numeric_dtype(df[f])]
        if non_numeric:
            raise ValueError(f"Non-numeric columns specified: {non_numeric}")

        return df[features], features
    else:
        # Auto-select numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if not numeric_cols:
            raise ValueError(
                "No numeric columns found in data. "
                "Specify features explicitly or ensure query returns numeric columns."
            )

        return df[numeric_cols], numeric_cols


def _generate_filter_sql(
    original_sql: str,
    id_column: str,
    ids: List[Any],
    limit: Optional[int] = None
) -> str:
    """
    Helper: Generate SQL to filter to specific IDs.

    Args:
        original_sql: Original query
        id_column: Column name for filtering
        ids: List of ID values
        limit: Optional limit on results

    Returns:
        SQL query string
    """
    # Quote IDs appropriately (assume string IDs for safety)
    id_list = "', '".join(str(id) for id in ids[:1000])  # Limit to 1000 IDs in SQL

    filter_sql = f'SELECT * FROM ({original_sql}) AS subq WHERE "{id_column}" IN (\'{id_list}\')'

    if limit:
        filter_sql += f" LIMIT {limit}"

    return filter_sql


def detect_outliers(
    sql: str,
    id_column: Optional[str] = None,
    method: str = "isolation_forest",
    contamination: float = 0.1,
    features: Optional[List[str]] = None,
    n_samples: int = DEFAULT_SAMPLE_SIZE
) -> Dict[str, Any]:
    """
    Detect statistical outliers in SQL query results.

    Args:
        sql: SQL query returning data to analyze
        id_column: Column name to use as identifier (for follow-up queries)
        method: Detection method ("isolation_forest" or "zscore")
        contamination: Expected proportion of outliers (0.0-0.5, default 0.1)
        features: Specific columns to analyze (None = all numeric columns)
        n_samples: Maximum number of outlier examples to return (default 20)

    Returns:
        Dictionary with outlier statistics, sample outliers, and follow-up SQL
    """
    try:
        # Fetch data
        df = _fetch_data(sql)
        logger.info(f"Starting outlier detection with method={method}, contamination={contamination}")

        # Select features
        X_df, feature_names = _select_features(df, features)
        X = X_df.values

        # Detect outliers
        if method == "isolation_forest":
            detector = IsolationForest(
                contamination=contamination,
                random_state=42,
                n_estimators=100
            )
            predictions = detector.fit_predict(X)  # -1 = outlier, 1 = normal
            scores = detector.score_samples(X)  # Lower = more anomalous
            outliers_mask = predictions == -1

        elif method == "zscore":
            # Z-score method (multivariate)
            z_scores = np.abs(stats.zscore(X, axis=0, nan_policy='omit'))
            threshold = 3.0  # Standard 3-sigma rule
            outliers_mask = (z_scores > threshold).any(axis=1)
            scores = -z_scores.max(axis=1)  # Negative for consistency with isolation forest

        else:
            return {"error": f"Unknown method: {method}. Use 'isolation_forest' or 'zscore'."}

        # Add results to dataframe
        df['anomaly_score'] = scores
        df['is_outlier'] = outliers_mask

        # Get outliers
        outlier_df = df[outliers_mask].copy()
        n_outliers = len(outlier_df)

        # Sort by score (most anomalous first)
        outlier_df = outlier_df.sort_values('anomaly_score')

        # Prepare result
        result = {
            "method": method,
            "n_total_rows": len(df),
            "n_outliers": n_outliers,
            "outlier_percentage": round(n_outliers / len(df) * 100, 2),
            "features_analyzed": feature_names,
            "score_statistics": {
                "min": float(scores.min()),
                "max": float(scores.max()),
                "mean": float(scores.mean()),
                "std": float(scores.std()),
                "quartiles": [float(q) for q in np.percentile(scores, [25, 50, 75])]
            }
        }

        # Add sample outliers
        sample_df = outlier_df.head(n_samples)
        result["top_outliers"] = sample_df.to_dict(orient='records')

        # Add outlier IDs if id_column provided
        if id_column:
            if id_column not in df.columns:
                result["warning"] = f"ID column '{id_column}' not found in results"
            else:
                outlier_ids = outlier_df[id_column].tolist()
                result["outlier_ids"] = outlier_ids[:1000]  # Limit to 1000

                # Generate follow-up SQL
                if outlier_ids:
                    result["follow_up_sql"] = _generate_filter_sql(
                        sql, id_column, outlier_ids
                    )

        logger.info(f"Outlier detection complete: {n_outliers} outliers found ({result['outlier_percentage']}%)")
        return result

    except Exception as e:
        logger.error(f"Error in outlier detection: {e}", exc_info=True)
        return {"error": str(e)}


def cluster_analysis(
    sql: str,
    method: str = "kmeans",
    n_clusters: int = 5,
    eps: float = 0.5,
    min_samples: int = 5,
    features: Optional[List[str]] = None,
    id_column: Optional[str] = None,
    n_samples_per_cluster: int = 5
) -> Dict[str, Any]:
    """
    Perform clustering analysis on SQL query results.

    Args:
        sql: SQL query returning data to analyze
        method: Clustering method ("kmeans" or "dbscan")
        n_clusters: Number of clusters for k-means (default 5)
        eps: Distance threshold for DBSCAN (default 0.5)
        min_samples: Minimum samples per cluster for DBSCAN (default 5)
        features: Specific columns to analyze (None = all numeric columns)
        id_column: Column name to use as identifier
        n_samples_per_cluster: Number of sample rows per cluster (default 5)

    Returns:
        Dictionary with cluster statistics, samples, and follow-up SQL
    """
    try:
        # Fetch data
        df = _fetch_data(sql)
        logger.info(f"Starting cluster analysis with method={method}")

        # Select and standardize features
        X_df, feature_names = _select_features(df, features)

        # Standardize features (critical for distance-based clustering)
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_df.values)

        # Perform clustering
        if method == "kmeans":
            clusterer = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            labels = clusterer.fit_predict(X_scaled)

            # Transform centroids back to original scale
            centroids_scaled = clusterer.cluster_centers_
            centroids = scaler.inverse_transform(centroids_scaled)
            inertia = clusterer.inertia_

        elif method == "dbscan":
            clusterer = DBSCAN(eps=eps, min_samples=min_samples)
            labels = clusterer.fit_predict(X_scaled)

            # DBSCAN uses -1 for noise points
            n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
            centroids = None
            inertia = None

        else:
            return {"error": f"Unknown method: {method}. Use 'kmeans' or 'dbscan'."}

        # Add cluster labels to dataframe
        df['cluster'] = labels

        # Calculate silhouette score (if enough clusters and not too many points)
        silhouette = None
        if len(set(labels)) > 1 and len(df) < 10000:
            try:
                silhouette = silhouette_score(X_scaled, labels)
            except Exception as e:
                logger.warning(f"Could not calculate silhouette score: {e}")

        # Cluster statistics
        cluster_stats = []
        cluster_samples = {}
        cluster_assignments = {}

        for cluster_id in sorted(set(labels)):
            cluster_df = df[df['cluster'] == cluster_id]
            cluster_size = len(cluster_df)

            stats_dict = {
                "cluster_id": int(cluster_id),
                "size": cluster_size,
                "percentage": round(cluster_size / len(df) * 100, 2)
            }

            # Add feature means for this cluster
            for i, feat in enumerate(feature_names):
                stats_dict[f"{feat}_mean"] = float(cluster_df[feat].mean())
                stats_dict[f"{feat}_std"] = float(cluster_df[feat].std())

            # Add centroid if available
            if centroids is not None and cluster_id >= 0:
                stats_dict["centroid"] = {
                    feat: float(centroids[cluster_id][i])
                    for i, feat in enumerate(feature_names)
                }

            cluster_stats.append(stats_dict)

            # Add sample rows
            sample = cluster_df.head(n_samples_per_cluster)
            cluster_samples[str(cluster_id)] = sample.to_dict(orient='records')

            # Add IDs if available
            if id_column and id_column in df.columns:
                cluster_assignments[str(cluster_id)] = cluster_df[id_column].tolist()[:1000]

        # Build result
        result = {
            "method": method,
            "n_clusters": n_clusters if method == "kmeans" else len([c for c in cluster_stats if c['cluster_id'] >= 0]),
            "n_total_rows": len(df),
            "features_analyzed": feature_names,
            "cluster_statistics": cluster_stats,
            "cluster_samples": cluster_samples
        }

        if silhouette is not None:
            result["silhouette_score"] = round(float(silhouette), 3)
            result["silhouette_interpretation"] = (
                "excellent" if silhouette > 0.7 else
                "good" if silhouette > 0.5 else
                "fair" if silhouette > 0.25 else
                "poor"
            )

        if inertia is not None:
            result["inertia"] = float(inertia)

        # Add follow-up SQL by cluster
        if id_column and id_column in df.columns:
            result["cluster_assignments"] = cluster_assignments
            result["follow_up_sql_by_cluster"] = {
                cid: _generate_filter_sql(sql, id_column, ids)
                for cid, ids in cluster_assignments.items()
                if ids  # Only if cluster has members
            }

        logger.info(f"Clustering complete: {result['n_clusters']} clusters identified")
        return result

    except Exception as e:
        logger.error(f"Error in cluster analysis: {e}", exc_info=True)
        return {"error": str(e)}


def correlation_analysis(
    sql: str,
    features: Optional[List[str]] = None,
    method: str = "pearson",
    threshold: float = 0.0
) -> Dict[str, Any]:
    """
    Calculate correlation matrix for SQL query results.

    Args:
        sql: SQL query returning data to analyze
        features: Specific columns to correlate (None = all numeric columns)
        method: Correlation method ("pearson" or "spearman")
        threshold: Only return correlations with absolute value above threshold (default 0.0 = all)

    Returns:
        Dictionary with correlation matrix and strong correlations
    """
    try:
        # Fetch data
        df = _fetch_data(sql)
        logger.info(f"Starting correlation analysis with method={method}")

        # Select features
        X_df, feature_names = _select_features(df, features)

        # Calculate correlation matrix
        if method == "pearson":
            corr_matrix = X_df.corr(method='pearson')
        elif method == "spearman":
            corr_matrix = X_df.corr(method='spearman')
        else:
            return {"error": f"Unknown method: {method}. Use 'pearson' or 'spearman'."}

        # Convert to dict
        corr_dict = corr_matrix.to_dict()

        # Find strong correlations
        strong_corr = []
        for i, feat1 in enumerate(feature_names):
            for j, feat2 in enumerate(feature_names):
                if i < j:  # Only upper triangle (avoid duplicates)
                    corr_val = corr_matrix.loc[feat1, feat2]

                    if abs(corr_val) >= threshold:
                        # Interpret strength
                        abs_corr = abs(corr_val)
                        if abs_corr >= 0.9:
                            strength = "very strong"
                        elif abs_corr >= 0.7:
                            strength = "strong"
                        elif abs_corr >= 0.5:
                            strength = "moderate"
                        elif abs_corr >= 0.3:
                            strength = "weak"
                        else:
                            strength = "very weak"

                        direction = "positive" if corr_val > 0 else "negative"

                        strong_corr.append({
                            "feature1": feat1,
                            "feature2": feat2,
                            "correlation": round(float(corr_val), 3),
                            "interpretation": f"{strength} {direction}"
                        })

        # Sort by absolute correlation
        strong_corr.sort(key=lambda x: abs(x['correlation']), reverse=True)

        # Check for multicollinearity (VIF calculation would be expensive, use simple threshold)
        multicollinearity_flags = []
        for corr_info in strong_corr:
            if abs(corr_info['correlation']) > 0.9:
                multicollinearity_flags.append({
                    "features": [corr_info['feature1'], corr_info['feature2']],
                    "correlation": corr_info['correlation'],
                    "warning": "Very high correlation - possible multicollinearity"
                })

        result = {
            "method": method,
            "n_features": len(feature_names),
            "n_observations": len(df),
            "correlation_matrix": corr_dict,
            "strong_correlations": strong_corr[:50],  # Limit to top 50
        }

        if multicollinearity_flags:
            result["multicollinearity_flags"] = multicollinearity_flags

        logger.info(f"Correlation analysis complete: {len(strong_corr)} correlations above threshold")
        return result

    except Exception as e:
        logger.error(f"Error in correlation analysis: {e}", exc_info=True)
        return {"error": str(e)}
