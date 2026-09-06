# Schema `raw_crypto_data`

- Chứa dữ liệu thô về crypto.
- Dữ liệu lịch sử giá của nhóm `ohlcv_*` hiện có từ `2017`.
- Phân loại dữ liệu:
  - `Technical`: `ohlcv_3m`, `ohlcv_15m`, `ohlcv_1h`, `ohlcv_2h`, `ohlcv_4h`, `ohlcv_8h`, `ohlcv_1d`, `ohlcv_7d`
  - `Fundamental / market-wide`: `crypto_global_metrics`
  - `Metadata / reference`: `coin_marketcap_top100_info`, `cryptocurrency_top100`, `venue_market_rule_snapshot`
  - `Operational`: `ohlcv_checkpoint`
  - `Sentiment`: hiện chưa có bảng sentiment trong `crypto_dags`

Lưu ý:
- Thời gian bên dưới đang mô tả theo `UTC`.
- Quan hệ giữa các bảng crypto chủ yếu đi bằng `symbol`, `exchange`, `timestamp`; không có foreign key vật lý.

## 1. Bảng `crypto_global_metrics`

### Mô tả
- Chứa snapshot toàn thị trường crypto theo ngày.
- Đây là bảng market-wide, không gắn với từng coin riêng lẻ.

### Nguồn dữ liệu
- CoinMarketCap API.

### Loại data
- `Fundamental / market-wide`

### Tần suất dữ liệu
- Dữ liệu ngày.

### Thời gian cập nhật
- Snapshot hàng ngày lúc `05:00 UTC`.

### Cấu trúc

| Cột | Kiểu dữ liệu | Ý nghĩa |
| --- | --- | --- |
| `timestamp` | `bigint` | Snapshot time chính, primary key |
| `btc_dominance` | `double precision` | BTC dominance |
| `eth_dominance` | `double precision` | ETH dominance |
| `active_cryptocurrencies` | `integer` | Số crypto active |
| `active_exchanges` | `integer` | Số exchange active |
| `active_market_pairs` | `integer` | Số market pair active |
| `total_market_cap` | `numeric(30,2)` | Tổng market cap |
| `total_volume_24h` | `numeric(30,2)` | Tổng volume 24h |
| `total_volume_24h_reported` | `numeric(30,2)` | Reported volume 24h |
| `altcoin_market_cap` | `numeric(30,2)` | Altcoin market cap |
| `altcoin_volume_24h` | `numeric(30,2)` | Altcoin volume 24h |
| `altcoin_volume_24h_reported` | `numeric(30,2)` | Reported altcoin volume 24h |
| `quote_timestamp` | `bigint` | Timestamp quote gốc từ nguồn |

### Relationship
- Bảng này độc lập, không có link bằng `id`.
- Khi phân tích có thể join theo thời gian với các bảng giá, nhưng đó là link logic theo `timestamp`, không phải FK.

## 2. Bảng `ohlcv_3m`

### Mô tả
- Bảng giá nến gốc của hệ crypto.
- Đây là nguồn base để aggregate lên các timeframe lớn hơn.

### Nguồn dữ liệu
- CCXT.

### Loại data
- `Technical`

### Tần suất dữ liệu
- Dữ liệu 3 phút.

### Thời gian cập nhật
- `3 phút / lần`.

### Data bắt đầu từ bao giờ
- Dữ liệu lịch sử giá hiện có từ `2017`.

### Exchange
- Các sàn hiện dùng: `binance`, `gateio`, `bitget`, `kucoin`, `bybit`, `okx`.
- Ưu tiên các sàn lớn như `binance`, `okx`, `kucoin`.

### Cấu trúc

| Cột | Kiểu dữ liệu | Ý nghĩa |
| --- | --- | --- |
| `symbol` | `varchar(100)` | Mã coin |
| `timestamp` | `bigint` | Thời điểm mở nến |
| `open` | `numeric(38,10)` | Giá mở cửa |
| `high` | `numeric(38,10)` | Giá cao nhất |
| `low` | `numeric(38,10)` | Giá thấp nhất |
| `close` | `numeric(38,10)` | Giá đóng cửa |
| `volume` | `numeric(38,10)` | Khối lượng |
| `datetime` | `timestamp without time zone` | Mốc thời gian dễ đọc |
| `exchange` | `varchar(100)` | Sàn giao dịch |
| `updated_at` | `timestamptz` | Thời điểm candle gần nhất thực sự thay đổi trong DB; rerun cùng OHLCV không làm đổi giá trị này |

### Khóa chính
- `(symbol, timestamp, exchange)`

### Relationship
- Là bảng nguồn để sinh ra:
  - `ohlcv_15m`
  - `ohlcv_1h`
  - `ohlcv_2h`
  - `ohlcv_4h`
  - `ohlcv_8h`
  - `ohlcv_1d`
  - `ohlcv_7d`
- Link logic với `ohlcv_checkpoint` qua:
  - `symbol`
  - `exchange`
  - `timeframe = '3m'`

## 3. Nhóm bảng aggregate từ `ohlcv_3m`

Các bảng:
- `ohlcv_15m`
- `ohlcv_1h`
- `ohlcv_2h`
- `ohlcv_4h`
- `ohlcv_8h`
- `ohlcv_1d`
- `ohlcv_7d`

### Mô tả
- Tất cả các bảng này đều được aggregate từ `ohlcv_3m`.
- Cấu trúc logic giống nhau, chỉ khác timeframe.

### Nguồn dữ liệu
- Không gọi API trực tiếp.
- Dữ liệu được tính lại từ `raw_crypto_data.ohlcv_3m`.

### Loại data
- `Technical`

### Tần suất dữ liệu và lịch cập nhật

| Table | Grain | Thời gian cập nhật |
| --- | --- | --- |
| `ohlcv_15m` | 15 phút | mỗi `15` phút |
| `ohlcv_1h` | 1 giờ | mỗi `1` giờ |
| `ohlcv_2h` | 2 giờ | mỗi `2` giờ |
| `ohlcv_4h` | 4 giờ | mỗi `4` giờ |
| `ohlcv_8h` | 8 giờ | mỗi `8` giờ |
| `ohlcv_1d` | 1 ngày | mỗi ngày |
| `ohlcv_7d` | 7 ngày | mỗi tuần |

### Data bắt đầu từ bao giờ
- Về business, các bảng này kế thừa lịch sử từ `ohlcv_3m`.
- Vì dữ liệu giá crypto đang có từ `2017`, nên nhóm `ohlcv_*` cũng có thể được xem là có lịch sử từ `2017`.

### Cấu trúc

Các bảng này có cùng structure:

| Cột | Kiểu dữ liệu | Ý nghĩa |
| --- | --- | --- |
| `symbol` | `varchar(100)` | Mã coin |
| `exchange` | `varchar(100)` | Sàn giao dịch |
| `timestamp` | `bigint` | Thời điểm mở nến |
| `open` | `numeric` | Giá mở cửa |
| `high` | `numeric` | Giá cao nhất |
| `low` | `numeric` | Giá thấp nhất |
| `close` | `numeric` | Giá đóng cửa |
| `volume` | `numeric` | Khối lượng |
| `datetime` | `timestamp without time zone` | Mốc thời gian dễ đọc |
| `updated_at` | `timestamptz` | Thời điểm candle aggregate gần nhất thực sự thay đổi trong DB |

### Khóa chính
- `(symbol, exchange, timestamp)`

### Relationship
- Tất cả đều link ngược về `ohlcv_3m` bằng:
  - `symbol`
  - `exchange`
  - `timestamp` sau khi bucket theo timeframe tương ứng
- Không có `id` riêng cho từng candle.

## 4. Bảng `cryptocurrency_market_metrics_daily`

### Mô tả
- Snapshot giá và market metrics theo coin và ngày từ CoinGecko.
- Khóa business là `(coin_id, metric_date)`.
- Pipeline dùng upsert; rerun cùng price/market metrics vẫn có thể làm mới metadata nguồn nhưng không thay đổi `updated_at`.

### Cột thời gian quan trọng

| Cột | Ý nghĩa |
| --- | --- |
| `metric_date` | Ngày business của snapshot |
| `snapshot_at` | Timestamp snapshot từ nguồn |
| `source_last_synced_at` | Thời điểm nguồn được đồng bộ |
| `updated_at` | Thời điểm normalized price hoặc market metrics gần nhất thực sự thay đổi trong DB |

Các payload `sources`, `quality` và timestamp nguồn không tự làm thay đổi `updated_at`.

## 5. Bảng `ohlcv_checkpoint`

### Mô tả
- Bảng vận hành để lưu checkpoint thời điểm đồng bộ dữ liệu OHLCV.
- Dùng để tránh cập nhật trùng lặp.
- Dùng để xác định mốc `last_ts_ms` cho lần sync tiếp theo.
- Có thể hỗ trợ tự chạy lại / backfill khi DB có vấn đề.

### Nguồn dữ liệu
- Dữ liệu nội bộ do pipeline sync OHLCV ghi vào.

### Loại data
- `Operational`

### Tần suất dữ liệu
- Không phải bảng business time series.
- Được cập nhật mỗi lần sync hoặc backfill thành công.

### Cấu trúc

| Cột | Kiểu dữ liệu | Ý nghĩa |
| --- | --- | --- |
| `symbol` | `varchar(100)` | Mã coin |
| `exchange` | `varchar(100)` | Sàn giao dịch |
| `timeframe` | `varchar(20)` | Khung thời gian đang checkpoint |
| `last_ts_ms` | `bigint` | Timestamp cuối cùng đã sync |
| `updated_at` | `timestamptz` | Thời điểm cập nhật checkpoint |

### Relationship
- Link logic với `ohlcv_3m` bằng:
  - `symbol`
  - `exchange`
  - `timeframe`
- Không link bằng `id`.

## 6. Bảng metadata hỗ trợ

### 6.1. `coin_marketcap_top100_info`

- Loại data: `Metadata / reference`
- Nguồn: metadata CoinMarketCap
- Vai trò:
  - lưu thông tin coin top 100
  - lưu classification flag
  - lưu `available_exchange` để chọn sàn sync OHLCV
- Link chính với các bảng giá bằng `symbol`
- Không có link bằng `id`

Cột chính:
- `symbol`
- `name`
- `marketcap`
- `payment`
- `smart_contract_platform`
- `stablecoin`
- `privacy_coin`
- `utility_token`
- `governar`
- `meme_coin`
- `nft`
- `available_exchange`

### 6.2. `venue_market_rule_snapshot`

- Loại data: `Metadata / reference`, một snapshot cho mỗi ngày UTC, venue và market.
- Nguồn: CCXT `load_markets()` (response chuẩn hóa và response gốc của từng venue).
- Tần suất: mỗi ngày lúc `00:20 UTC`; ưu tiên snapshot pair USDT, fallback USDC khi không có pair USDT, cho các venue trong `available_exchange`.
- Venue được lấy trực tiếp từ `available_exchange`; không có allow-list cấu hình.
- Migration cần chạy trước khi bật DAG: `migrations/20260726_create_venue_market_rule_snapshot.sql`.

| Cột | Kiểu dữ liệu | Ý nghĩa |
| --- | --- | --- |
| `venue` | `varchar(100)` | CCXT exchange id |
| `market_symbol` | `varchar(255)` | Unified market symbol, ví dụ `BTC/USDT` |
| `asset_symbol` | `varchar(100)` | Asset theo metadata nội bộ |
| `base_asset`, `quote_asset` | `varchar(100)` | Hai tài sản của market |
| `active` | `boolean` | Market có đang active trên venue không |
| `amount_step`, `price_tick` | `numeric` | Bước quantity và price hợp lệ |
| `min_amount`, `max_amount` | `numeric` | Giới hạn quantity của order |
| `min_notional`, `max_notional` | `numeric` | Giới hạn giá trị order (`price × amount`) |
| `precision_mode` | `varchar(32)` | Cách CCXT biểu diễn precision (`tick_size`, `decimal_places`, `significant_digits`) |
| `raw_info` | `jsonb` | CCXT normalized market object và raw venue response |
| `captured_at` | `date` | Ngày capture theo UTC |

Khóa chính là `(captured_at, venue, market_symbol)`. Retry hoặc rerun trong cùng ngày UTC update lại đúng snapshot đó.

Truy vấn rule gần nhất có hiệu lực tại một thời điểm:

```sql
SELECT DISTINCT ON (venue, market_symbol) *
FROM raw_crypto_data.venue_market_rule_snapshot
WHERE captured_at <= DATE '2026-07-26'
ORDER BY venue, market_symbol, captured_at DESC;
```

Manual run có thể filter payload: `{"venues": ["binance"], "symbols": ["BTC", "ETH"]}`. Hai filter đều optional và chỉ thu hẹp tập pair đang ingest.

### 6.3. CMC Top 30 point-in-time universe

- `cmc_top30_universe_runs`: audit từng lần ingest snapshot cuối tháng, gồm trạng thái, số lần thử, lỗi, credit, payload hash và thời gian nguồn.
- `cmc_top30_universe_snapshot`: đúng 30 asset/rank cho mỗi month-end thành công; khóa `(snapshot_date, cmc_id)`, rank là duy nhất trong ngày.
- `cmc_top30_membership_history`: biểu diễn snapshot thành interval ngày `[valid_from, valid_to)`; snapshot cuối tháng có hiệu lực từ ngày 1 tháng kế tiếp.
- `cmc_top30_membership_history_ts`: interval timestamp theo đúng thời điểm công bố `00:30 UTC`.
- `cmc_top30_universe_as_of(DATE)`: API SQL lấy universe tại một ngày; nếu thiếu month-end thì carry forward snapshot thành công gần nhất.
- `cmc_top30_universe_as_of(TIMESTAMPTZ)`: API PIT không look-ahead; `00:29 UTC` vẫn dùng snapshot cũ, `00:30 UTC` mới chuyển snapshot.
- `cmc_top30_symbol_targets`: canonical symbol, sàn USDT đã pin, cutoff dữ liệu, policy stable/wrapped và trạng thái OHLCV.
- `cmc_top30_data_coverage`: coverage theo `(symbol, selected_exchange)`, gồm requested/actual range, freshness và lỗi gần nhất.
- `cmc_top30_investable_universe_as_of(TIMESTAMPTZ)`: loại stablecoin/wrapped, yêu cầu symbol mapping và closed 3m candle trong sáu phút.
- `cmc_top30_ohlcv_{3m,15m,1h,2h,4h,8h,1d,7d}_pit`: candle của đúng canonical symbol và selected exchange trong interval membership.
- `cmc_top30_current_universe`: universe theo `CURRENT_DATE`.
- `cmc_top30_universe_gaps`: các month-end trong cửa sổ Builder đang missing, pending, incomplete hoặc có lần refresh gần nhất thất bại.

Raw universe giữ nguyên CMC rank 1–30 với `cryptocurrency_type=all`, nên vẫn gồm stablecoin/wrapped. `cmc_id` chỉ tồn tại ở lớp raw để lưu đúng source và phát hiện đổi symbol. Market data dùng schema legacy `(symbol, exchange, timestamp)`, quote cố định USDT và một sàn được pin theo priority. Alias cũ không được đưa vào Top 30 price view; prefix không có trên sàn được ghi nhận bằng actual coverage thay vì candle zero.


## 7. Relationship tổng thể

### Link logic chính

| Bảng trái | Bảng phải | Key link |
| --- | --- | --- |
| `coin_marketcap_top100_info` | `ohlcv_3m` | `symbol` + `available_exchange -> exchange` |
| `cmc_top30_universe_snapshot` | metadata CMC | `cmc_id` |
| `cmc_top30_universe_snapshot` | `cmc_top30_symbol_targets` | canonical `symbol`; `cmc_id` chỉ dùng tạm để collapse alias |
| `cmc_top30_symbol_targets` | các bảng OHLCV | `symbol`, `selected_exchange -> exchange`, `data_start_at` |
| `coin_marketcap_top100_info` | `venue_market_rule_snapshot` | `symbol` + `available_exchange -> venue`, quote `USDT` |
| `ohlcv_3m` | `ohlcv_15m/1h/2h/4h/8h/1d/7d` | `symbol`, `exchange`, bucket timestamp |
| `ohlcv_checkpoint` | `ohlcv_3m` | `symbol`, `exchange`, `timeframe` |

### Có link bằng `id` không?

- Hầu như không.
- `cryptocurrency_top100.id` là `id` duy nhất có ý nghĩa reference.
- Consumer legacy và CMC Top 30 Phase 1 cùng dùng natural key cũ:
  - `symbol`
  - `exchange`
  - `timestamp`
  - `timeframe`
