import numpy as np
from sklearn.decomposition import TruncatedSVD


def get_weights_using_svd_decom(num_components, standardized_features):
    svd = TruncatedSVD(n_components=num_components, random_state=45)
    svd.fit(standardized_features)
    svd_components = svd.components_
    feature_contributions = np.abs(svd_components)
    weights = np.sum(feature_contributions, axis=0) / np.sum(feature_contributions)
    return weights


def get_best_n_components_svd(
    standardized_features, max_components=None, threshold=0.95
):
    if max_components is None:
        max_components = standardized_features.shape[1]

    explained_variances = []

    for num_components in range(1, max_components + 1):
        svd = TruncatedSVD(n_components=num_components, random_state=45)
        svd.fit(standardized_features)
        explained_variances.append(sum(svd.explained_variance_ratio_))

        if explained_variances[-1] >= threshold:
            break
    return explained_variances.index(max(explained_variances)) + 1
