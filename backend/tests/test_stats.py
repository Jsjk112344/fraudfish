"""Tests for IQR-based statistical outlier detection."""

from agents.stats import compute_market_stats


class TestBasicStats:
    def test_basic_stats(self):
        prices = [320, 450, 480, 520, 550, 560, 580, 600, 620, 650, 700, 950]
        result = compute_market_stats(prices, 150)
        assert result["average_price"] == 581.67
        assert result["median_price"] == 570.0
        assert result["min_price"] == 320
        assert result["max_price"] == 950
        assert result["is_outlier"] is True
        assert result["outlier_direction"] == "suspiciously_low"

    def test_percentile(self):
        result = compute_market_stats([100, 200, 300, 400, 500], 250)
        # 2 out of 5 prices are below 250 (100, 200)
        assert result["this_listing_percentile"] == 40

    def test_deviation(self):
        result = compute_market_stats([100, 200, 300, 400, 500], 150)
        # median is 300, deviation = (150 - 300) / 300 * 100 = -50.0
        assert result["price_deviation_percent"] < 0
        assert result["price_deviation_percent"] == -50.0

    def test_outlier_high(self):
        result = compute_market_stats([100, 200, 300, 400, 500], 2000)
        assert result["is_outlier"] is True
        assert result["outlier_direction"] == "suspiciously_high"

    def test_not_outlier(self):
        result = compute_market_stats([100, 200, 300, 400, 500], 300)
        assert result["is_outlier"] is False
        assert result["outlier_direction"] is None

    def test_insufficient_data(self):
        result = compute_market_stats([100, 200], 150)
        assert result["average_price"] is None
        assert result["median_price"] is None
        assert result["min_price"] is None
        assert result["max_price"] is None
        assert result["this_listing_percentile"] is None
        assert result["price_deviation_percent"] is None
        assert result["is_outlier"] is False

    def test_empty_prices(self):
        result = compute_market_stats([], 150)
        assert result["average_price"] is None
        assert result["median_price"] is None
        assert result["is_outlier"] is False
