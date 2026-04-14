use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

// ============ arc_bottom 原有函数 ============

#[pyfunction]
fn scan_single_symbol(
    py: Python,
    symbol: String,
    open: Vec<f64>,
    high: Vec<f64>,
    low: Vec<f64>,
    close: Vec<f64>,
    timestamps: Vec<String>,
    quote_vol: Vec<f64>,
    min_history: usize,
    lookback_hours: usize,
    right_bull_bars: usize,
    box_min_bars: usize,
    box_max_amp: f64,
    left_min_bars: usize,
    left_max_bulls: usize,
    min_drop_pct: f64,
    max_drop_pct: f64,
) -> PyResult<Option<PyObject>> {
    let total_len = close.len();
    if total_len < min_history {
        return Ok(None);
    }

    let start_idx = if total_len > lookback_hours { total_len - lookback_hours } else { 0 };
    let n = total_len - start_idx;
    
    if n < left_min_bars + box_min_bars + right_bull_bars {
        return Ok(None);
    }

    // Right bars
    let mut is_breakout = true;
    for i in (n - right_bull_bars)..n {
        let idx = start_idx + i;
        if close[idx] <= open[idx] {
            is_breakout = false;
            break;
        }
    }
    if !is_breakout {
        return Ok(None);
    }

    for box_len in box_min_bars..24 {
        let box_start = n as isize - right_bull_bars as isize - box_len as isize;
        let box_end = n as isize - right_bull_bars as isize - 1;
        if box_start < 0 {
            break;
        }
        let box_start = box_start as usize;
        let box_end = box_end as usize;

        let mut box_high = f64::MIN;
        let mut box_low = f64::MAX;
        for i in box_start..=box_end {
            let idx = start_idx + i;
            if high[idx] > box_high { box_high = high[idx]; }
            if low[idx] < box_low { box_low = low[idx]; }
        }

        if box_low <= 0.0 { continue; }
        let box_amp = (box_high - box_low) / box_low;
        if box_amp > box_max_amp { continue; }

        for left_len in left_min_bars..24 {
            let left_start = box_start as isize - left_len as isize;
            let left_end = box_start as isize - 1;
            if left_start < 0 { break; }
            let left_start = left_start as usize;
            let left_end = left_end as usize;

            let mut bullish_count = 0;
            for i in left_start..=left_end {
                let idx = start_idx + i;
                if close[idx] > open[idx] {
                    bullish_count += 1;
                }
            }
            if bullish_count > left_max_bulls { continue; }

            let start_body_high = open[start_idx + left_start].max(close[start_idx + left_start]);
            let end_body_low = open[start_idx + left_end].min(close[start_idx + left_end]);
            if start_body_high <= 0.0 { continue; }

            let drop_pct = (start_body_high - end_body_low) / start_body_high;

            if drop_pct >= min_drop_pct && drop_pct <= max_drop_pct {
                let c1_idx = start_idx + n - 1;
                
                // Build return dict
                let dict = PyDict::new_bound(py);
                dict.set_item("symbol", &symbol)?;
                dict.set_item("price", close[c1_idx])?;
                dict.set_item("vol", (quote_vol[c1_idx] / 1_000_000.0 * 100.0).round() / 100.0)?;
                
                let time_str = &timestamps[c1_idx];
                let time_parts: Vec<&str> = time_str.split(' ').collect();
                let md_hm = if time_parts.len() == 2 {
                    let date_parts: Vec<&str> = time_parts[0].split('-').collect();
                    let time_part = time_parts[1].chars().take(5).collect::<String>();
                    if date_parts.len() >= 3 {
                        format!("{}-{} {}", date_parts[1], date_parts[2], time_part)
                    } else {
                        time_str.clone()
                    }
                } else {
                    time_str.clone()
                };
                
                // Extract hour roughly
                let end_hour = if time_parts.len() == 2 {
                    let t_parts: Vec<&str> = time_parts[1].split(':').collect();
                    t_parts[0].parse::<u32>().unwrap_or(0)
                } else {
                    0
                };

                dict.set_item("endHour", end_hour)?;
                dict.set_item("time", md_hm)?;
                dict.set_item("drop_pct", (drop_pct * 10000.0).round() / 100.0)?;
                dict.set_item("box_amp", (box_amp * 10000.0).round() / 100.0)?;
                dict.set_item("left_len", left_len)?;
                dict.set_item("box_len", box_len)?;
                dict.set_item("is_watchlist", false)?;

                // details
                let details = pyo3::types::PyList::empty_bound(py);
                
                let d1 = PyDict::new_bound(py);
                d1.set_item("step", "右侧突破")?;
                d1.set_item("time", dict.get_item("time")?.unwrap())?;
                d1.set_item("pass", true)?;
                d1.set_item("reason", format!("最新 {} 小时连阳突破", right_bull_bars))?;
                details.append(d1)?;

                let d2 = PyDict::new_bound(py);
                d2.set_item("step", "底部盘整")?;
                let t_box_start = timestamps[start_idx + box_start].split(' ').nth(1).unwrap_or("").chars().take(5).collect::<String>();
                let t_box_end = timestamps[start_idx + box_end].split(' ').nth(1).unwrap_or("").chars().take(5).collect::<String>();
                d2.set_item("time", format!("{}~{}", t_box_start, t_box_end))?;
                d2.set_item("pass", true)?;
                d2.set_item("reason", format!("盘整 {} 小时, 振幅 {:.2}% (≤{:.0}%)", box_len, box_amp * 100.0, box_max_amp * 100.0))?;
                details.append(d2)?;

                let d3 = PyDict::new_bound(py);
                d3.set_item("step", "左侧下跌")?;
                let t_left_start = timestamps[start_idx + left_start].split(' ').nth(1).unwrap_or("").chars().take(5).collect::<String>();
                let t_left_end = timestamps[start_idx + left_end].split(' ').nth(1).unwrap_or("").chars().take(5).collect::<String>();
                d3.set_item("time", format!("{}~{}", t_left_start, t_left_end))?;
                d3.set_item("pass", true)?;
                d3.set_item("reason", format!("下跌 {} 小时, 跌幅 {:.2}%, 包含 {} 根反抽阳线", left_len, drop_pct * 100.0, bullish_count))?;
                details.append(d3)?;

                dict.set_item("details", details)?;
                
                return Ok(Some(dict.into()));
            }
        }
    }

    Ok(None)
}

// ============ strategy1 & strategy1_pro ============

struct BarInfo {
    o: f64,
    h: f64,
    l: f64,
    c: f64,
    vol: f64,
    range_pct: f64,
    ts_ms: i64,
}

fn format_ts(ts_ms: i64) -> String {
    use chrono::{TimeZone, Utc};
    match Utc.timestamp_millis_opt(ts_ms) {
        chrono::LocalResult::Single(dt) => dt.format("%m-%d %H:%M").to_string(),
        _ => String::from("??-?? ??:??"),
    }
}

fn build_result_dict(
    py: Python,
    symbol: &str,
    bars_info: &[BarInfo],
    consecutive_count: usize,
    is_watchlist: bool,
    watch_reason: &str,
) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    
    if bars_info.is_empty() {
        return Ok(dict.into());
    }
    
    let last = &bars_info[bars_info.len() - 1];
    let first = &bars_info[0];
    
    dict.set_item("symbol", symbol)?;
    dict.set_item("price", last.c)?;
    
    // 计算时间范围
    let first_ts = format_ts(first.ts_ms);
    let last_ts = format_ts(last.ts_ms);
    let time_str = format!("{} ~ {}", first_ts, last_ts);
    dict.set_item("time", time_str)?;
    dict.set_item("startTime", first_ts)?;
    dict.set_item("endTime", last_ts)?;
    
    // 计算 endHour
    let end_hour = ((last.ts_ms / 1000) / 3600) % 24;
    dict.set_item("endHour", end_hour as i32)?;
    
    dict.set_item("hrs", consecutive_count)?;
    dict.set_item("vol", (last.vol / 1_000_000.0 * 100.0).round() / 100.0)?;
    
    // 计算总涨幅
    let total_gain = ((last.c - first.o) / first.o * 100.0 * 100.0).round() / 100.0;
    dict.set_item("gain", total_gain)?;
    
    // bars 详情
    let bars_list = PyList::empty_bound(py);
    for bar in bars_info {
        let bar_dict = PyDict::new_bound(py);
        bar_dict.set_item("t", format_ts(bar.ts_ms))?;
        bar_dict.set_item("o", format!("{:.6}", bar.o))?;
        bar_dict.set_item("high", format!("{:.6}", bar.h))?;
        bar_dict.set_item("low", format!("{:.6}", bar.l))?;
        bar_dict.set_item("c", format!("{:.6}", bar.c))?;
        bar_dict.set_item("r", format!("{:.2}%", bar.range_pct * 100.0))?;
        bar_dict.set_item("v", format!("{:.2}M", bar.vol / 1_000_000.0))?;
        bar_dict.set_item("type", "阳线")?;
        bars_list.append(bar_dict)?;
    }
    dict.set_item("bars", bars_list)?;
    
    dict.set_item("is_watchlist", is_watchlist)?;
    if is_watchlist {
        dict.set_item("watch_reason", watch_reason)?;
    }
    
    Ok(dict.into())
}

fn scan_s1_core(
    py: Python,
    symbol: String,
    open_arr: Vec<f64>,
    high_arr: Vec<f64>,
    low_arr: Vec<f64>,
    close_arr: Vec<f64>,
    timestamps_ms: Vec<i64>,  // 改为 i64 Unix 毫秒时间戳
    quote_vol: Vec<f64>,
    cutoff_ts_ms: i64,
    min_hours: usize,
    pro_mode: bool,
    min_body_ratio: f64,
    max_single_gain: f64,
) -> PyResult<Option<PyObject>> {
    let n = close_arr.len();
    if n < 10 { return Ok(None); }

    // 找最近6根不超过 cutoff_ts_ms 的K线索引
    let mut valid_indices: Vec<usize> = Vec::new();
    for i in 0..n {
        let ts_ms = timestamps_ms[i];
        if ts_ms <= cutoff_ts_ms {
            valid_indices.push(i);
        }
    }
    
    // 取最后6个（从末尾往前取，然后反转保持时间顺序）
    let recent: Vec<usize> = valid_indices.iter().rev().take(6).cloned().rev().collect();
    if recent.len() < 3 { return Ok(None); }

    // 倒序遍历（从最新到最旧）
    let mut consecutive_count: usize = 0;
    let mut current_low = 0.0f64;
    let mut bars_info: Vec<BarInfo> = Vec::new();
    let mut is_watchlist = false;
    let mut watch_reason = String::new();

    for &i in recent.iter().rev() {
        let o = open_arr[i];
        let h = high_arr[i];
        let l = low_arr[i];
        let c = close_arr[i];
        let vol = quote_vol[i];
        let ts_ms = timestamps_ms[i];

        let is_bullish = c > o;
        if !is_bullish { break; }

        // PRO: 实体比例过滤
        if pro_mode {
            let body = (c - o).abs();
            let range = h - l + 1e-8;
            let body_ratio = body / range;
            if body_ratio < min_body_ratio { break; }
        }

        // 低点抬高检查
        if consecutive_count == 0 {
            current_low = l;
            consecutive_count = 1;
        } else {
            if l >= current_low { break; }
            current_low = l;
            consecutive_count += 1;
        }

        let range_pct = (h - l) / l;
        bars_info.insert(0, BarInfo { o, h, l, c, vol, range_pct, ts_ms });

        // PRO: 单根涨幅过滤（记录后再判断）
        if pro_mode {
            let single_gain = (c - o) / o;
            if single_gain > max_single_gain {
                is_watchlist = true;
                watch_reason = format!("单根涨幅 {:.2}% > {:.2}%", single_gain * 100.0, max_single_gain * 100.0);
                break;
            }
        }
    }

    // watchlist 直接返回（PRO only）
    if is_watchlist && pro_mode && !bars_info.is_empty() {
        let dict = build_result_dict(py, &symbol, &bars_info, consecutive_count, true, &watch_reason)?;
        return Ok(Some(dict));
    }
    
    if consecutive_count < min_hours { return Ok(None); }
    
    let dict = build_result_dict(py, &symbol, &bars_info, consecutive_count, false, "")?;
    Ok(Some(dict))
}

/// strategy1: 稳步抬升基础版
/// 返回 None 表示不符合条件，返回 Some(dict) 表示命中信号
#[pyfunction]
fn scan_strategy1_symbol(
    py: Python,
    symbol: String,
    open_arr: Vec<f64>,
    high_arr: Vec<f64>,
    low_arr: Vec<f64>,
    close_arr: Vec<f64>,
    timestamps_ms: Vec<i64>,  // 改为 i64 Unix 毫秒时间戳
    quote_vol: Vec<f64>,
    cutoff_ts_ms: i64,
    min_hours: usize,
) -> PyResult<Option<PyObject>> {
    scan_s1_core(py, symbol, open_arr, high_arr, low_arr, close_arr,
                 timestamps_ms, quote_vol, cutoff_ts_ms, min_hours,
                 false, 0.0, 1.0)
}

/// strategy1_pro: 增加实体比例和单根涨幅过滤
#[pyfunction]
fn scan_strategy1_pro_symbol(
    py: Python,
    symbol: String,
    open_arr: Vec<f64>,
    high_arr: Vec<f64>,
    low_arr: Vec<f64>,
    close_arr: Vec<f64>,
    timestamps_ms: Vec<i64>,  // 改为 i64 Unix 毫秒时间戳
    quote_vol: Vec<f64>,
    cutoff_ts_ms: i64,
    min_hours: usize,
    min_body_ratio: f64,
    max_single_gain: f64,
) -> PyResult<Option<PyObject>> {
    scan_s1_core(py, symbol, open_arr, high_arr, low_arr, close_arr,
                 timestamps_ms, quote_vol, cutoff_ts_ms, min_hours,
                 true, min_body_ratio, max_single_gain)
}

#[pymodule]
fn crypto_engine(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(scan_single_symbol, m)?)?;
    m.add_function(wrap_pyfunction!(scan_strategy1_symbol, m)?)?;
    m.add_function(wrap_pyfunction!(scan_strategy1_pro_symbol, m)?)?;
    Ok(())
}
