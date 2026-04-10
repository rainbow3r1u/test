use pyo3::prelude::*;
use pyo3::types::PyDict;

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

#[pymodule]
fn crypto_engine(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(scan_single_symbol, m)?)?;
    Ok(())
}
