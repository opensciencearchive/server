"""US6 footprint guard: the vector-index stack is gone from the image.

`chromadb` and `sentence-transformers` were the heaviest runtime deps; the
unified `/data/` surface replaced the vector/keyword index domain, so neither
should be importable. If a future change reintroduces them as a transitive
dependency, these tests fail loudly rather than silently re-bloating the image.
"""

import importlib

import pytest


@pytest.mark.parametrize("module", ["chromadb", "sentence_transformers"])
def test_index_dependency_not_importable(module: str):
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(module)


@pytest.mark.parametrize(
    "module",
    [
        "osa.domain.index",
        "osa.domain.search",
        "osa.domain.export",
        "osa.domain.discovery",
        "osa.infrastructure.index",
        "osa.sdk.index",
    ],
)
def test_deleted_domain_not_importable(module: str):
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(module)
