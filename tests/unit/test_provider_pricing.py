"""Provider pricing contract: exclusive modes, exact models, truthful estimates."""
import pytest
from pydantic import ValidationError

from loom.provider_pricing import ModelPrice, PricingConfig, calculate_price, parse_price_import


def test_models_are_independent_and_zero_is_a_price():
    config = PricingConfig(pricing_mode="custom", custom_pricing={
        "model-a": {"input_usd_per_1m": 0, "output_usd_per_1m": 0},
    })
    assert config.price_for("model-a") is not None
    assert config.price_for("model-b") is None
    assert config.price_for("models/model-a") is None


@pytest.mark.parametrize("value", [-1, float("inf"), float("nan"), True, "bad"])
def test_invalid_price_rejected(value):
    with pytest.raises(ValidationError):
        ModelPrice(input_usd_per_1m=value, output_usd_per_1m=1)


def test_base_prices_required_and_cache_is_optional():
    with pytest.raises(ValidationError):
        ModelPrice(input_usd_per_1m=1)
    price = ModelPrice(input_usd_per_1m=1, output_usd_per_1m=2)
    assert price.cache_read_usd_per_1m is None
    assert calculate_price(price, input_tokens=100, output_tokens=10,
                           cache_read_tokens=30) is None


def test_explicit_cache_overlap_avoids_double_billing():
    price = ModelPrice(input_usd_per_1m=10, output_usd_per_1m=20,
                       cache_read_usd_per_1m=1, cache_write_usd_per_1m=5)
    assert calculate_price(price, input_tokens=1000000, output_tokens=0,
                           cache_read_tokens=400000, input_includes_cache=True) == 6.4
    assert calculate_price(price, input_tokens=600000, output_tokens=0,
                           cache_read_tokens=400000, input_includes_cache=False) == 6.4


@pytest.mark.parametrize("mode", ["usage_only", "catalog"])
def test_no_mixed_catalog_and_custom(mode):
    with pytest.raises(ValidationError):
        PricingConfig(pricing_mode=mode, catalog_id="supplier:yibuapi" if mode == "catalog" else None,
                      custom_pricing={"a": {"input_usd_per_1m": 1, "output_usd_per_1m": 1}})


def test_import_rejects_duplicates_and_invalid_units_atomically():
    with pytest.raises(ValueError, match="duplicate"):
        parse_price_import('model,input_usd_per_1m,output_usd_per_1m\na,1,2\na,3,4', "csv")
    with pytest.raises(ValueError):
        parse_price_import('[{"model":"a","input_usd_per_1m":1,"output_usd_per_1m":2,"currency":"CNY"}]', "json")
    assert parse_price_import('model,input_usd_per_1m,output_usd_per_1m\na,0,2', "csv")["a"].input_usd_per_1m == 0
