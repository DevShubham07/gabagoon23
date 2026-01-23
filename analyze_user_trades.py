#!/usr/bin/env python3
"""
Analyze user trading activity and generate charts for each market slug.
"""

import json
import os
import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import numpy as np

# Set style for better-looking charts
try:
    plt.style.use('seaborn-v0_8-darkgrid')
except:
    try:
        plt.style.use('seaborn-darkgrid')
    except:
        plt.style.use('default')

def load_user_activity(file_path):
    """Load user activity JSON file."""
    with open(file_path, 'r') as f:
        return json.load(f)

def load_market_data(market_dir):
    """Load market data JSON file."""
    # Find the market data JSON file (not user_activity.json)
    json_files = [f for f in os.listdir(market_dir) if f.endswith('.json') and 'user_activity' not in f]
    if json_files:
        market_file = os.path.join(market_dir, json_files[0])
        try:
            with open(market_file, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            # If JSON is corrupted, log warning but continue without market data
            print(f"   ⚠️  Warning: Market data file {json_files[0]} is corrupted (JSON decode error), continuing without market data")
            return None
        except Exception as e:
            print(f"   ⚠️  Warning: Error loading market data: {str(e)}, continuing without market data")
            return None
    return None

def analyze_trades(transactions, merges_df=None):
    """Analyze trading data and extract metrics with merge-aware resets."""
    trades = [t for t in transactions if t.get('type') == 'TRADE']
    merges = [t for t in transactions if t.get('type') == 'MERGE']
    redeems = [t for t in transactions if t.get('type') == 'REDEEM']
    
    # Create DataFrame for trades
    trades_df = None
    if trades:
        trades_data = []
        for t in trades:
            trades_data.append({
                'timestamp': datetime.fromtimestamp(t.get('timestamp', 0)),
                'outcome': 'Up' if (t.get('outcome') == 'Up' or t.get('outcomeIndex') == 0) else 'Down',
                'size': t.get('size', 0),
                'price': t.get('price', 0),
                'usdcSize': t.get('usdcSize', 0),
                'side': t.get('side', ''),
            })
        trades_df = pd.DataFrame(trades_data)
        trades_df.set_index('timestamp', inplace=True)
        trades_df.sort_index(inplace=True)
    
    # Create DataFrame for merges
    merges_df = None
    if merges:
        merges_data = []
        for m in merges:
            merges_data.append({
                'timestamp': datetime.fromtimestamp(m.get('timestamp', 0)),
                'size': m.get('size', 0),
                'usdcSize': m.get('usdcSize', 0),
            })
        merges_df = pd.DataFrame(merges_data)
        merges_df.set_index('timestamp', inplace=True)
        merges_df.sort_index(inplace=True)
    
    # Create DataFrame for redeems
    redeems_df = None
    if redeems:
        redeems_data = []
        for r in redeems:
            redeems_data.append({
                'timestamp': datetime.fromtimestamp(r.get('timestamp', 0)),
                'size': r.get('size', 0),
                'usdcSize': r.get('usdcSize', 0),
            })
        redeems_df = pd.DataFrame(redeems_data)
        redeems_df.set_index('timestamp', inplace=True)
        redeems_df.sort_index(inplace=True)
    
    return {
        'trades_df': trades_df,
        'merges_df': merges_df,
        'redeems_df': redeems_df,
    }

def create_charts(data, market_slug, output_dir, market_data=None):
    """Create all prioritized charts for the market."""
    trades_df = data['trades_df']
    merges_df = data['merges_df']
    redeems_df = data['redeems_df']
    
    if trades_df is None or len(trades_df) == 0:
        print(f"   ⚠️  No trades found for {market_slug}")
        return
    
    # Determine market start time - use first trade as reference point
    # This ensures relative time starts at 0 for the actual trading period
    market_start = trades_df.index.min()
    
    # Try to align with market data if available, but prioritize trade alignment
    if market_data and 'startTime' in market_data:
        try:
            market_data_start = datetime.fromisoformat(market_data['startTime'].replace('Z', '+00:00')).replace(tzinfo=None)
            # Only use market data start if it's close to first trade (within 1 hour)
            time_diff = abs((trades_df.index.min() - market_data_start).total_seconds())
            if time_diff < 3600:  # Within 1 hour
                market_start = market_data_start
        except:
            pass
    
    # Convert all timestamps to relative time (seconds from market start)
    def to_relative_time(timestamp):
        return (timestamp - market_start).total_seconds()
    
    # Create relative time columns
    trades_df_rel = trades_df.copy()
    trades_df_rel['relative_time'] = trades_df_rel.index.map(to_relative_time)
    
    if merges_df is not None and len(merges_df) > 0:
        merges_df_rel = merges_df.copy()
        merges_df_rel['relative_time'] = merges_df_rel.index.map(to_relative_time)
    else:
        merges_df_rel = None
    
    if redeems_df is not None and len(redeems_df) > 0:
        redeems_df_rel = redeems_df.copy()
        redeems_df_rel['relative_time'] = redeems_df_rel.index.map(to_relative_time)
    else:
        redeems_df_rel = None
    
    # Get time range in relative seconds (0 to 900 seconds = 15 minutes)
    start_rel = 0
    max_rel_time = trades_df_rel['relative_time'].max()
    min_rel_time = trades_df_rel['relative_time'].min()
    
    # Ensure relative times start from 0 (shift if needed)
    if min_rel_time < 0:
        shift = abs(min_rel_time)
        trades_df_rel['relative_time'] += shift
        if merges_df_rel is not None:
            merges_df_rel['relative_time'] += shift
        if redeems_df_rel is not None:
            redeems_df_rel['relative_time'] += shift
        max_rel_time = trades_df_rel['relative_time'].max()
    elif min_rel_time > 0:
        # Shift all times to start from 0
        shift = -min_rel_time
        trades_df_rel['relative_time'] += shift
        if merges_df_rel is not None:
            merges_df_rel['relative_time'] += shift
        if redeems_df_rel is not None:
            redeems_df_rel['relative_time'] += shift
        max_rel_time = trades_df_rel['relative_time'].max()
    
    # Cap at 15 minutes (900 seconds) - filter trades beyond this
    trades_df_rel = trades_df_rel[trades_df_rel['relative_time'] <= 900].copy()
    
    # Keep original merges/redeems for analysis (don't filter them for analysis)
    # But filter for charts to only show those within 900 seconds
    merges_df_rel_orig = merges_df_rel.copy() if merges_df_rel is not None else None
    redeems_df_rel_orig = redeems_df_rel.copy() if redeems_df_rel is not None else None
    
    if merges_df_rel is not None:
        merges_df_rel = merges_df_rel[merges_df_rel['relative_time'] <= 900].copy()
    if redeems_df_rel is not None:
        redeems_df_rel = redeems_df_rel[redeems_df_rel['relative_time'] <= 900].copy()
    
    # Recalculate max after filtering
    if len(trades_df_rel) > 0:
        max_rel_time = trades_df_rel['relative_time'].max()
        x_max = min(900, max(60, max_rel_time))
    else:
        x_max = 900  # Default to 15 minutes if no trades
    
    time_range_rel = np.arange(start_rel, min(x_max + 30, 900), 30)  # 30-second intervals, max 900 seconds
    
    # Helper function to format relative time as MM:SS
    def format_relative_time(seconds):
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}:{secs:02d}"
    
    # Chart 1: Trade scatter (execution view) - IMPROVED
    # Ensure x_max is reasonable (max 15 minutes = 900 seconds)
    x_max = min(900, max(60, min(max_rel_time, 900)))
    if x_max > 900:
        x_max = 900
    
    fig1, ax1 = plt.subplots(figsize=(16, 8))
    
    # Plot trades colored by outcome with better visualization
    up_trades = trades_df_rel[trades_df_rel['outcome'] == 'Up']
    down_trades = trades_df_rel[trades_df_rel['outcome'] == 'Down']
    
    # Normalize marker sizes (scale to reasonable range)
    max_usdc = trades_df['usdcSize'].max()
    min_usdc = trades_df['usdcSize'].min()
    if max_usdc > min_usdc:
        marker_sizes_up = 30 + (up_trades['usdcSize'] - min_usdc) / (max_usdc - min_usdc) * 400
        marker_sizes_down = 30 + (down_trades['usdcSize'] - min_usdc) / (max_usdc - min_usdc) * 400
    else:
        marker_sizes_up = [100] * len(up_trades)
        marker_sizes_down = [100] * len(down_trades)
    
    # Plot with better styling (using relative time)
    scatter1 = ax1.scatter(up_trades['relative_time'], up_trades['price'], s=marker_sizes_up, 
                          c='#2ecc71', alpha=0.7, label='Up Trades', edgecolors='darkgreen', linewidths=1)
    scatter2 = ax1.scatter(down_trades['relative_time'], down_trades['price'], s=marker_sizes_down, 
                          c='#e74c3c', alpha=0.7, label='Down Trades', edgecolors='darkred', linewidths=1)
    
    # Overlay MERGE events
    if merges_df_rel is not None and len(merges_df_rel) > 0:
        for idx, merge_row in merges_df_rel.iterrows():
            merge_rel_time = merge_row['relative_time']
            ax1.axvline(x=merge_rel_time, color='purple', linestyle='--', alpha=0.8, linewidth=3, zorder=10)
            ax1.text(merge_rel_time, ax1.get_ylim()[1] * 0.98, f'MERGE\n${merge_row["usdcSize"]:.2f}', 
                    rotation=90, ha='right', va='top', fontsize=9, fontweight='bold',
                    bbox=dict(boxstyle='round,pad=0.5', facecolor='purple', alpha=0.4, edgecolor='purple'))
    
    # Overlay REDEEM events
    if redeems_df_rel is not None and len(redeems_df_rel) > 0:
        for idx, redeem_row in redeems_df_rel.iterrows():
            redeem_rel_time = redeem_row['relative_time']
            ax1.axvline(x=redeem_rel_time, color='orange', linestyle='--', alpha=0.8, linewidth=3, zorder=10)
            ax1.text(redeem_rel_time, ax1.get_ylim()[0] * 1.02, f'REDEEM\n${redeem_row["usdcSize"]:.2f}', 
                    rotation=90, ha='left', va='bottom', fontsize=9, fontweight='bold',
                    bbox=dict(boxstyle='round,pad=0.5', facecolor='orange', alpha=0.4, edgecolor='orange'))
    
    ax1.set_xlabel('Time (MM:SS)', fontsize=13, fontweight='bold')
    ax1.set_ylabel('Price', fontsize=13, fontweight='bold')
    ax1.set_title('Trade Execution View: Price vs Time\n(Marker size = USDC Size)', 
                  fontsize=15, fontweight='bold', pad=20)
    ax1.legend(loc='best', fontsize=11, framealpha=0.9)
    ax1.grid(True, alpha=0.3, linestyle='--')
    
    # Set x-axis to show relative time (0:00 to 15:00)
    ax1.set_xlim(0, x_max)
    tick_positions = np.arange(0, x_max + 60, 60)  # Every minute
    tick_positions = tick_positions[tick_positions <= x_max]
    ax1.set_xticks(tick_positions)
    ax1.set_xticklabels([format_relative_time(t) for t in tick_positions], rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '1_trade_scatter_execution.png'), dpi=150, bbox_inches='tight')
    plt.close()
    
    # Chart 3: Cumulative exposure / position lines (with merge resets)
    fig2, (ax2a, ax2b, ax2c) = plt.subplots(3, 1, figsize=(18, 14))
    
    # Calculate volume and counts with merge resets (using relative time)
    up_volume_list = []
    down_volume_list = []
    trade_counts_list = []
    time_buckets_rel = []
    
    current_up_volume = 0
    current_down_volume = 0
    current_trade_count = 0
    
    for bucket_start_rel in time_range_rel:
        bucket_end_rel = bucket_start_rel + 30
        
        # Check if merge happened before this bucket (reset)
        if merges_df_rel is not None and len(merges_df_rel) > 0:
            merges_before = merges_df_rel[merges_df_rel['relative_time'] < bucket_start_rel]
            if len(merges_before) > 0:
                # Reset if merge happened since last bucket
                last_merge_rel = merges_before['relative_time'].max()
                if bucket_start_rel - last_merge_rel < 30:
                    current_up_volume = 0
                    current_down_volume = 0
                    current_trade_count = 0
        
        # Get trades in this bucket (using relative time)
        bucket_trades = trades_df_rel[(trades_df_rel['relative_time'] >= bucket_start_rel) & 
                                      (trades_df_rel['relative_time'] < bucket_end_rel)]
        
        if len(bucket_trades) > 0:
            up_vol = bucket_trades[bucket_trades['outcome'] == 'Up']['usdcSize'].sum()
            down_vol = bucket_trades[bucket_trades['outcome'] == 'Down']['usdcSize'].sum()
            current_up_volume += up_vol
            current_down_volume += down_vol
            current_trade_count += len(bucket_trades)
        
        up_volume_list.append(current_up_volume)
        down_volume_list.append(current_down_volume)
        trade_counts_list.append(current_trade_count)
        time_buckets_rel.append(bucket_start_rel)
    
    up_volume_series = pd.Series(up_volume_list, index=time_buckets_rel)
    down_volume_series = pd.Series(down_volume_list, index=time_buckets_rel)
    trade_counts_series = pd.Series(trade_counts_list, index=time_buckets_rel)
    
    # Plot for different aggregations
    for ax, title_suffix in [(ax2a, '30-second buckets'), 
                              (ax2b, '5-minute aggregated'), 
                              (ax2c, '15-minute aggregated')]:
        if '30-second' in title_suffix:
            up_vol = up_volume_series
            down_vol = down_volume_series
            counts = trade_counts_series
            x_pos = range(len(up_vol))
            n_show = max(1, len(x_pos) // 30)
        elif '5-minute' in title_suffix:
            # Resample by taking every 10th bucket (10 * 30s = 5min)
            up_vol = up_volume_series.iloc[::10]
            down_vol = down_volume_series.iloc[::10]
            counts = trade_counts_series.iloc[::10]
            x_pos = range(len(up_vol))
            n_show = max(1, len(x_pos) // 10)
        else:  # 15-minute
            # Resample by taking every 30th bucket (30 * 30s = 15min)
            up_vol = up_volume_series.iloc[::30]
            down_vol = down_volume_series.iloc[::30]
            counts = trade_counts_series.iloc[::30]
            x_pos = range(len(up_vol))
            n_show = max(1, len(x_pos) // 5)
        
        width = 0.8
        
        ax.bar(x_pos, up_vol.values, width, label='Up Volume', color='#2ecc71', alpha=0.7)
        ax.bar(x_pos, down_vol.values, width, bottom=up_vol.values, 
              label='Down Volume', color='#e74c3c', alpha=0.7)
        
        # Secondary axis for trade count
        ax2_twin = ax.twinx()
        ax2_twin.plot(x_pos, counts.values, color='black', marker='o', 
                     markersize=4, linewidth=2, label='Trade Count', alpha=0.7)
        ax2_twin.set_ylabel('Trade Count', fontsize=11, color='black', fontweight='bold')
        ax2_twin.tick_params(axis='y', labelcolor='black')
        
        ax.set_xlabel('Time Bucket', fontsize=11, fontweight='bold')
        ax.set_ylabel('Cumulative USDC Volume', fontsize=11, fontweight='bold')
        ax.set_title(f'Volume & Activity Bars ({title_suffix})', fontsize=13, fontweight='bold')
        ax.legend(loc='upper left', fontsize=10)
        ax2_twin.legend(loc='upper right', fontsize=10)
        ax.grid(True, alpha=0.3, axis='y')
        
        # Set x-axis to use actual relative time values
        ax.set_xlim(0, x_max)
        tick_positions = np.arange(0, x_max + 60, 60)  # Every minute
        tick_positions = tick_positions[tick_positions <= x_max]
        ax.set_xticks(tick_positions)
        ax.set_xticklabels([format_relative_time(t) for t in tick_positions], rotation=45, ha='right', fontsize=9)
        ax.set_xlabel('Time (MM:SS)', fontsize=11, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '2_volume_activity_bars.png'), dpi=150, bbox_inches='tight')
    plt.close()
    
    # Chart 2: Cumulative exposure / position lines (with merge resets)
    fig2, (ax2a, ax2b) = plt.subplots(2, 1, figsize=(18, 12))
    
    # Calculate cumulative with merge resets (using relative time)
    up_cum_size = []
    down_cum_size = []
    up_cum_usdc = []
    down_cum_usdc = []
    cum_times_rel = []
    
    current_up_size = 0
    current_down_size = 0
    current_up_usdc = 0
    current_down_usdc = 0
    
    for idx, trade_row in trades_df_rel.iterrows():
        trade_rel_time = trade_row['relative_time']
        
        # Check if merge happened before this trade
        if merges_df_rel is not None and len(merges_df_rel) > 0:
            merges_before = merges_df_rel[merges_df_rel['relative_time'] < trade_rel_time]
            if len(merges_before) > 0:
                last_merge_rel = merges_before['relative_time'].max()
                # Reset if merge happened recently (within 1 second)
                if trade_rel_time - last_merge_rel < 1:
                    current_up_size = 0
                    current_down_size = 0
                    current_up_usdc = 0
                    current_down_usdc = 0
        
        if trade_row['outcome'] == 'Up':
            current_up_size += trade_row['size']
            current_up_usdc += trade_row['usdcSize']
        else:
            current_down_size += trade_row['size']
            current_down_usdc += trade_row['usdcSize']
        
        up_cum_size.append(current_up_size)
        down_cum_size.append(current_down_size)
        up_cum_usdc.append(current_up_usdc)
        down_cum_usdc.append(current_down_usdc)
        cum_times_rel.append(trade_rel_time)
    
    # Plot cumulative size (using relative time)
    ax2a.plot(cum_times_rel, up_cum_size, label='Up Cumulative Size', 
             color='#2ecc71', linewidth=2.5, marker='o', markersize=3, alpha=0.8)
    ax2a.plot(cum_times_rel, down_cum_size, label='Down Cumulative Size', 
             color='#e74c3c', linewidth=2.5, marker='s', markersize=3, alpha=0.8)
    
    # Mark MERGE events
    if merges_df_rel is not None and len(merges_df_rel) > 0:
        for idx, merge_row in merges_df_rel.iterrows():
            merge_rel_time = merge_row['relative_time']
            ax2a.axvline(x=merge_rel_time, color='purple', linestyle='--', alpha=0.8, linewidth=3, zorder=10)
            ax2a.scatter([merge_rel_time], [ax2a.get_ylim()[1] * 0.9], marker='*', 
                        s=300, color='purple', zorder=10, edgecolors='black', linewidths=1)
    
    # Mark REDEEM events
    if redeems_df_rel is not None and len(redeems_df_rel) > 0:
        for idx, redeem_row in redeems_df_rel.iterrows():
            redeem_rel_time = redeem_row['relative_time']
            ax2a.axvline(x=redeem_rel_time, color='orange', linestyle='--', alpha=0.8, linewidth=3, zorder=10)
            ax2a.scatter([redeem_rel_time], [ax2a.get_ylim()[1] * 0.95], marker='*', 
                        s=300, color='orange', zorder=10, edgecolors='black', linewidths=1)
    
    ax2a.set_xlabel('Time (MM:SS)', fontsize=12, fontweight='bold')
    ax2a.set_ylabel('Cumulative Size', fontsize=12, fontweight='bold')
    ax2a.set_title('Cumulative Position Size by Outcome (with Merge Resets)', fontsize=14, fontweight='bold')
    ax2a.legend(loc='best', fontsize=11)
    ax2a.grid(True, alpha=0.3)
    ax2a.set_xlim(0, x_max)
    tick_positions = np.arange(0, x_max + 60, 60)
    tick_positions = tick_positions[tick_positions <= x_max]
    ax2a.set_xticks(tick_positions)
    ax2a.set_xticklabels([format_relative_time(t) for t in tick_positions], rotation=45, ha='right')
    
    # Plot cumulative USDC (using relative time)
    ax2b.plot(cum_times_rel, up_cum_usdc, label='Up Cumulative USDC', 
             color='#2ecc71', linewidth=2.5, marker='o', markersize=3, alpha=0.8)
    ax2b.plot(cum_times_rel, down_cum_usdc, label='Down Cumulative USDC', 
             color='#e74c3c', linewidth=2.5, marker='s', markersize=3, alpha=0.8)
    
    # Mark MERGE and REDEEM events
    if merges_df_rel is not None and len(merges_df_rel) > 0:
        for idx, merge_row in merges_df_rel.iterrows():
            merge_rel_time = merge_row['relative_time']
            ax2b.axvline(x=merge_rel_time, color='purple', linestyle='--', alpha=0.8, linewidth=3, zorder=10)
            ax2b.scatter([merge_rel_time], [ax2b.get_ylim()[1] * 0.9], marker='*', 
                        s=300, color='purple', zorder=10, edgecolors='black', linewidths=1)
    
    if redeems_df_rel is not None and len(redeems_df_rel) > 0:
        for idx, redeem_row in redeems_df_rel.iterrows():
            redeem_rel_time = redeem_row['relative_time']
            ax2b.axvline(x=redeem_rel_time, color='orange', linestyle='--', alpha=0.8, linewidth=3, zorder=10)
            ax2b.scatter([redeem_rel_time], [ax2b.get_ylim()[1] * 0.95], marker='*', 
                        s=300, color='orange', zorder=10, edgecolors='black', linewidths=1)
    
    ax2b.set_xlabel('Time (MM:SS)', fontsize=12, fontweight='bold')
    ax2b.set_ylabel('Cumulative USDC', fontsize=12, fontweight='bold')
    ax2b.set_title('Cumulative USDC Exposure by Outcome (with Merge Resets)', fontsize=14, fontweight='bold')
    ax2b.legend(loc='best', fontsize=11)
    ax2b.grid(True, alpha=0.3)
    ax2b.set_xlim(0, x_max)
    tick_positions = np.arange(0, x_max + 60, 60)
    tick_positions = tick_positions[tick_positions <= x_max]
    ax2b.set_xticks(tick_positions)
    ax2b.set_xticklabels([format_relative_time(t) for t in tick_positions], rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '2_cumulative_exposure.png'), dpi=150, bbox_inches='tight')
    plt.close()
    
    # Chart 3: VWAP / rolling average price by outcome
    fig3, (ax3a, ax3b) = plt.subplots(2, 1, figsize=(18, 12))
    
    window_sizes = [10, 50]
    
    for window_size in window_sizes:
        ax = ax3a if window_size == 10 else ax3b
        
        # VWAP for Up trades (using relative time)
        up_trades = trades_df_rel[trades_df_rel['outcome'] == 'Up'].copy()
        if len(up_trades) >= window_size:
            up_trades['vwap'] = (up_trades['price'] * up_trades['usdcSize']).rolling(window=window_size).sum() / \
                               up_trades['usdcSize'].rolling(window=window_size).sum()
            ax.plot(up_trades['relative_time'], up_trades['vwap'], label=f'Up VWAP (window={window_size})', 
                   color='#2ecc71', linewidth=2.5, alpha=0.9)
        
        # VWAP for Down trades (using relative time)
        down_trades = trades_df_rel[trades_df_rel['outcome'] == 'Down'].copy()
        if len(down_trades) >= window_size:
            down_trades['vwap'] = (down_trades['price'] * down_trades['usdcSize']).rolling(window=window_size).sum() / \
                                 down_trades['usdcSize'].rolling(window=window_size).sum()
            ax.plot(down_trades['relative_time'], down_trades['vwap'], label=f'Down VWAP (window={window_size})', 
                   color='#e74c3c', linewidth=2.5, alpha=0.9)
        
        # Also plot actual prices for reference
        ax.scatter(up_trades['relative_time'], up_trades['price'], s=15, alpha=0.3, color='#2ecc71', label='Up Prices', zorder=1)
        ax.scatter(down_trades['relative_time'], down_trades['price'], s=15, alpha=0.3, color='#e74c3c', label='Down Prices', zorder=1)
        
        ax.set_xlabel('Time (MM:SS)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Price / VWAP', fontsize=12, fontweight='bold')
        ax.set_title(f'VWAP / Rolling Average Price (window={window_size} trades)', fontsize=13, fontweight='bold')
        ax.legend(loc='best', fontsize=10)
        ax.grid(True, alpha=0.3)
        x_max = min(900, max(60, max_rel_time))
        ax.set_xlim(0, x_max)
        tick_positions = np.arange(0, x_max + 60, 60)
        tick_positions = tick_positions[tick_positions <= x_max]
        ax.set_xticks(tick_positions)
        ax.set_xticklabels([format_relative_time(t) for t in tick_positions], rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '3_vwap_rolling_average.png'), dpi=150, bbox_inches='tight')
    plt.close()
    
    # Chart 4: Price vs size scatter + regression
    fig4, (ax4a, ax4b) = plt.subplots(1, 2, figsize=(18, 8))
    
    up_trades = trades_df_rel[trades_df_rel['outcome'] == 'Up']
    if len(up_trades) > 0:
        ax4a.scatter(up_trades['price'], up_trades['size'], alpha=0.6, s=80, color='#2ecc71', edgecolors='darkgreen', linewidths=0.5)
        
        if len(up_trades) > 1:
            z = np.polyfit(up_trades['price'], up_trades['size'], 1)
            p = np.poly1d(z)
            x_line = np.linspace(up_trades['price'].min(), up_trades['price'].max(), 100)
            ax4a.plot(x_line, p(x_line), "r--", alpha=0.8, linewidth=2.5, label=f'Trend: y={z[0]:.2f}x+{z[1]:.2f}')
            
            corr = np.corrcoef(up_trades['price'], up_trades['size'])[0, 1]
            ax4a.text(0.05, 0.95, f'Correlation: {corr:.3f}', transform=ax4a.transAxes,
                     fontsize=11, verticalalignment='top', 
                     bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.7))
        
        ax4a.set_xlabel('Price', fontsize=12, fontweight='bold')
        ax4a.set_ylabel('Size', fontsize=12, fontweight='bold')
        ax4a.set_title('Up Trades: Price vs Size', fontsize=13, fontweight='bold')
        ax4a.legend(loc='best', fontsize=10)
        ax4a.grid(True, alpha=0.3)
    
    down_trades = trades_df_rel[trades_df_rel['outcome'] == 'Down']
    if len(down_trades) > 0:
        ax4b.scatter(down_trades['price'], down_trades['size'], alpha=0.6, s=80, color='#e74c3c', edgecolors='darkred', linewidths=0.5)
        
        if len(down_trades) > 1:
            z = np.polyfit(down_trades['price'], down_trades['size'], 1)
            p = np.poly1d(z)
            x_line = np.linspace(down_trades['price'].min(), down_trades['price'].max(), 100)
            ax4b.plot(x_line, p(x_line), "r--", alpha=0.8, linewidth=2.5, label=f'Trend: y={z[0]:.2f}x+{z[1]:.2f}')
            
            corr = np.corrcoef(down_trades['price'], down_trades['size'])[0, 1]
            ax4b.text(0.05, 0.95, f'Correlation: {corr:.3f}', transform=ax4b.transAxes,
                     fontsize=11, verticalalignment='top',
                     bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.7))
        
        ax4b.set_xlabel('Price', fontsize=12, fontweight='bold')
        ax4b.set_ylabel('Size', fontsize=12, fontweight='bold')
        ax4b.set_title('Down Trades: Price vs Size', fontsize=13, fontweight='bold')
        ax4b.legend(loc='best', fontsize=10)
        ax4b.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '4_price_vs_size_regression.png'), dpi=150, bbox_inches='tight')
    plt.close()
    
    # Chart 5: Histogram of trade sizes
    fig5, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    axes[0, 0].hist(trades_df_rel['size'], bins=50, color='#3498db', alpha=0.7, edgecolor='black')
    axes[0, 0].set_xlabel('Trade Size', fontsize=11, fontweight='bold')
    axes[0, 0].set_ylabel('Frequency', fontsize=11, fontweight='bold')
    axes[0, 0].set_title('Trade Size Distribution (Linear Scale)', fontsize=12, fontweight='bold')
    axes[0, 0].grid(True, alpha=0.3, axis='y')
    
    axes[0, 1].hist(trades_df_rel['size'], bins=50, color='#3498db', alpha=0.7, edgecolor='black')
    axes[0, 1].set_xscale('log')
    axes[0, 1].set_xlabel('Trade Size (log scale)', fontsize=11, fontweight='bold')
    axes[0, 1].set_ylabel('Frequency', fontsize=11, fontweight='bold')
    axes[0, 1].set_title('Trade Size Distribution (Log Scale)', fontsize=12, fontweight='bold')
    axes[0, 1].grid(True, alpha=0.3, axis='y')
    
    weights = trades_df_rel['usdcSize'] / trades_df_rel['usdcSize'].sum() * len(trades_df_rel)
    axes[1, 0].hist(trades_df_rel['size'], bins=50, weights=weights, color='#9b59b6', alpha=0.7, edgecolor='black')
    axes[1, 0].set_xlabel('Trade Size', fontsize=11, fontweight='bold')
    axes[1, 0].set_ylabel('Weighted Frequency (by USDC)', fontsize=11, fontweight='bold')
    axes[1, 0].set_title('Trade Size Distribution (Weighted by USDC Size)', fontsize=12, fontweight='bold')
    axes[1, 0].grid(True, alpha=0.3, axis='y')
    
    axes[1, 1].hist(trades_df_rel['usdcSize'], bins=50, color='#e67e22', alpha=0.7, edgecolor='black')
    axes[1, 1].set_xlabel('USDC Size', fontsize=11, fontweight='bold')
    axes[1, 1].set_ylabel('Frequency', fontsize=11, fontweight='bold')
    axes[1, 1].set_title('USDC Size Distribution', fontsize=12, fontweight='bold')
    axes[1, 1].grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '5_trade_size_histograms.png'), dpi=150, bbox_inches='tight')
    plt.close()
    
    # Generate analysis text file
    def format_time(seconds):
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}:{secs:02d}"
    
    # Use original (unfiltered) merges/redeems for analysis
    generate_analysis_text(trades_df_rel, merges_df_rel_orig, redeems_df_rel_orig, market_slug, output_dir, x_max, format_time)

def generate_analysis_text(trades_df_rel, merges_df_rel, redeems_df_rel, market_slug, output_dir, x_max, format_time):
    """Generate analysis text file with variables between start-merges-redeem segments."""
    if trades_df_rel is None or len(trades_df_rel) == 0:
        return
    
    # Define segment boundaries
    segments = []
    segments.append({'start': 0, 'end': None, 'type': 'START', 'name': 'Start'})
    
    # Add merge points
    merge_count = 0
    if merges_df_rel is not None and len(merges_df_rel) > 0:
        for idx, merge_row in merges_df_rel.iterrows():
            merge_count += 1
            segments.append({
                'start': merge_row['relative_time'],
                'end': None,
                'type': 'MERGE',
                'name': f'MERGE {merge_count}',
                'usdcSize': merge_row['usdcSize']
            })
    
    # Add redeem point
    if redeems_df_rel is not None and len(redeems_df_rel) > 0:
        for idx, redeem_row in redeems_df_rel.iterrows():
            segments.append({
                'start': redeem_row['relative_time'],
                'end': None,
                'type': 'REDEEM',
                'name': 'REDEEM',
                'usdcSize': redeem_row['usdcSize']
            })
    
    # Sort segments by start time
    segments.sort(key=lambda x: x['start'])
    
    # Set end times for segments
    # Use the last trade time as reference
    last_trade_time = trades_df_rel['relative_time'].max() if len(trades_df_rel) > 0 else x_max
    
    for i in range(len(segments)):
        if i < len(segments) - 1:
            # End at the next segment start
            segments[i]['end'] = segments[i+1]['start']
        else:
            # Last segment ends at the last trade time (or merge/redeem time if after trades)
            segments[i]['end'] = max(segments[i]['start'], last_trade_time)
    
    # Analyze each segment
    analysis_lines = []
    analysis_lines.append("=" * 80)
    analysis_lines.append(f"TRADING ANALYSIS: {market_slug}")
    analysis_lines.append("=" * 80)
    analysis_lines.append("")
    
    # Market timeline summary
    analysis_lines.append("MARKET TIMELINE:")
    analysis_lines.append(f"  Market Start: {format_time(0)}")
    
    # Get all merges and redeems (even if filtered out)
    all_merges = []
    all_redeems = []
    if merges_df_rel is not None and len(merges_df_rel) > 0:
        for idx, merge_row in merges_df_rel.iterrows():
            merge_time = merge_row['relative_time']
            all_merges.append((merge_time, merge_row['usdcSize']))
    
    if redeems_df_rel is not None and len(redeems_df_rel) > 0:
        for idx, redeem_row in redeems_df_rel.iterrows():
            redeem_time = redeem_row['relative_time']
            all_redeems.append((redeem_time, redeem_row['usdcSize']))
    
    # Sort by time
    all_merges.sort(key=lambda x: x[0])
    all_redeems.sort(key=lambda x: x[0])
    
    merge_num = 1
    for merge_time, merge_usdc in all_merges:
        if merge_time <= 900:  # Within 15 minutes
            analysis_lines.append(f"  MERGE {merge_num} occurred at: {format_time(merge_time)} (USDC: ${merge_usdc:.2f})")
        else:
            # Show even if beyond 15 minutes
            minutes = int(merge_time // 60)
            secs = int(merge_time % 60)
            analysis_lines.append(f"  MERGE {merge_num} occurred at: {minutes}:{secs:02d} (USDC: ${merge_usdc:.2f}) [After market end]")
        merge_num += 1
    
    for redeem_time, redeem_usdc in all_redeems:
        if redeem_time <= 900:  # Within 15 minutes
            analysis_lines.append(f"  REDEEM occurred at: {format_time(redeem_time)} (USDC: ${redeem_usdc:.2f})")
        else:
            # Show even if beyond 15 minutes
            minutes = int(redeem_time // 60)
            secs = int(redeem_time % 60)
            analysis_lines.append(f"  REDEEM occurred at: {minutes}:{secs:02d} (USDC: ${redeem_usdc:.2f}) [After market end]")
    
    analysis_lines.append(f"  Market End (last trade): {format_time(x_max)}")
    analysis_lines.append("")
    
    for seg_idx, segment in enumerate(segments):
        start_time = segment['start']
        end_time = segment['end']
        
        # Get trades in this segment
        segment_trades = trades_df_rel[
            (trades_df_rel['relative_time'] >= start_time) & 
            (trades_df_rel['relative_time'] < end_time)
        ]
        
        # Always show segments, even if no trades (for MERGE/REDEEM segments)
        
        analysis_lines.append("-" * 80)
        analysis_lines.append(f"SEGMENT {seg_idx + 1}: {segment['name']}")
        analysis_lines.append("-" * 80)
        analysis_lines.append(f"Time Range: {format_time(start_time)} - {format_time(end_time)}")
        analysis_lines.append(f"Duration: {format_time(end_time - start_time)}")
        
        if segment['type'] == 'MERGE':
            analysis_lines.append(f"MERGE USDC Size: ${segment['usdcSize']:.2f}")
        elif segment['type'] == 'REDEEM':
            analysis_lines.append(f"REDEEM USDC Size: ${segment['usdcSize']:.2f}")
        
        if len(segment_trades) > 0:
            up_trades = segment_trades[segment_trades['outcome'] == 'Up']
            down_trades = segment_trades[segment_trades['outcome'] == 'Down']
            
            # Basic counts
            analysis_lines.append("")
            analysis_lines.append("TRADE COUNTS:")
            analysis_lines.append(f"  Total Trades: {len(segment_trades)}")
            analysis_lines.append(f"  Up Trades: {len(up_trades)}")
            analysis_lines.append(f"  Down Trades: {len(down_trades)}")
            
            # Average sizes
            if len(up_trades) > 0:
                avg_up_size = up_trades['size'].mean()
                analysis_lines.append(f"  Average Up Trade Size: {avg_up_size:.4f}")
            if len(down_trades) > 0:
                avg_down_size = down_trades['size'].mean()
                analysis_lines.append(f"  Average Down Trade Size: {avg_down_size:.4f}")
            if len(up_trades) > 0 and len(down_trades) > 0:
                size_ratio = avg_up_size / avg_down_size if avg_down_size > 0 else 0
                analysis_lines.append(f"  Size Ratio (Up/Down): {size_ratio:.4f}")
            
            # Average prices
            if len(up_trades) > 0:
                avg_up_price = up_trades['price'].mean()
                analysis_lines.append(f"  Average Up Price: {avg_up_price:.6f}")
            if len(down_trades) > 0:
                avg_down_price = down_trades['price'].mean()
                analysis_lines.append(f"  Average Down Price: {avg_down_price:.6f}")
            if len(up_trades) > 0 and len(down_trades) > 0:
                sum_avg = avg_up_price + avg_down_price
                analysis_lines.append(f"  Sum of Averages (Up + Down): {sum_avg:.6f}")
            
            # USDC spent
            usdc_up = up_trades['usdcSize'].sum() if len(up_trades) > 0 else 0
            usdc_down = down_trades['usdcSize'].sum() if len(down_trades) > 0 else 0
            total_usdc = usdc_up + usdc_down
            
            analysis_lines.append("")
            analysis_lines.append("USDC SPENT:")
            analysis_lines.append(f"  USDC Spent on Up: ${usdc_up:.2f}")
            analysis_lines.append(f"  USDC Spent on Down: ${usdc_down:.2f}")
            analysis_lines.append(f"  Total USDC Spent: ${total_usdc:.2f}")
            
            # Volume
            volume_up = up_trades['size'].sum() if len(up_trades) > 0 else 0
            volume_down = down_trades['size'].sum() if len(down_trades) > 0 else 0
            
            analysis_lines.append("")
            analysis_lines.append("VOLUME:")
            analysis_lines.append(f"  Volume Up: {volume_up:.4f}")
            analysis_lines.append(f"  Volume Down: {volume_down:.4f}")
            analysis_lines.append(f"  Total Volume: {volume_up + volume_down:.4f}")
            
            # Price statistics
            if len(segment_trades) > 0:
                analysis_lines.append("")
                analysis_lines.append("PRICE STATISTICS:")
                analysis_lines.append(f"  Min Price: {segment_trades['price'].min():.6f}")
                analysis_lines.append(f"  Max Price: {segment_trades['price'].max():.6f}")
                analysis_lines.append(f"  Price Range: {segment_trades['price'].max() - segment_trades['price'].min():.6f}")
                analysis_lines.append(f"  Price Std Dev: {segment_trades['price'].std():.6f}")
        else:
            # No trades in this segment - show segment info anyway
            analysis_lines.append("")
            analysis_lines.append("TRADE COUNTS:")
            analysis_lines.append(f"  Total Trades: 0")
            analysis_lines.append(f"  Up Trades: 0")
            analysis_lines.append(f"  Down Trades: 0")
            analysis_lines.append("")
            analysis_lines.append("USDC SPENT:")
            analysis_lines.append(f"  USDC Spent on Up: $0.00")
            analysis_lines.append(f"  USDC Spent on Down: $0.00")
            analysis_lines.append(f"  Total USDC Spent: $0.00")
            analysis_lines.append("")
            analysis_lines.append("VOLUME:")
            analysis_lines.append(f"  Volume Up: 0.0000")
            analysis_lines.append(f"  Volume Down: 0.0000")
            analysis_lines.append(f"  Total Volume: 0.0000")
        
        analysis_lines.append("")
    
    # Write to file
    analysis_file = os.path.join(output_dir, 'analysis.txt')
    with open(analysis_file, 'w') as f:
        f.write('\n'.join(analysis_lines))
    
    print(f"   ✓ Generated analysis.txt")


def process_market(market_dir):
    """Process a single market directory."""
    user_activity_file = os.path.join(market_dir, 'user_activity.json')
    
    if not os.path.exists(user_activity_file):
        print(f"⚠️  No user_activity.json found in {market_dir}")
        return False
    
    print(f"📊 Processing: {os.path.basename(market_dir)}")
    
    try:
        # Load data
        data = load_user_activity(user_activity_file)
        market_slug = data.get('marketSlug', os.path.basename(market_dir))
        
        # Load market data
        market_data = load_market_data(market_dir)
        
        # Analyze
        analyzed_data = analyze_trades(data.get('transactions', []))
        
        # Create charts
        create_charts(analyzed_data, market_slug, market_dir, market_data)
        
        print(f"   ✓ Generated charts for {market_slug}")
        return True
        
    except Exception as e:
        print(f"   ❌ Error processing {market_dir}: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def needs_processing(market_dir):
    """Check if a market directory needs processing (has user_activity.json but no analysis.txt)."""
    user_activity_file = os.path.join(market_dir, 'user_activity.json')
    analysis_file = os.path.join(market_dir, 'analysis.txt')
    
    # Needs processing if user_activity.json exists but analysis.txt doesn't
    return os.path.exists(user_activity_file) and not os.path.exists(analysis_file)

def main():
    """Main function."""
    if len(sys.argv) > 1:
        # Process specific directory
        market_dir = sys.argv[1]
        if os.path.isdir(market_dir):
            process_market(market_dir)
        else:
            print(f"Error: {market_dir} is not a directory")
    else:
        # Process all markets in the directory
        base_dir = '/Users/shubham.1/Applications/untitled folder/data/2026/01'
        
        if not os.path.exists(base_dir):
            print(f"Error: Base directory not found: {base_dir}")
            return
        
        # Get all subdirectories
        all_items = os.listdir(base_dir)
        market_dirs = [os.path.join(base_dir, item) for item in all_items 
                      if os.path.isdir(os.path.join(base_dir, item)) and 
                      item.startswith('btc-updown-15m-')]
        
        # Filter to only those that need processing
        markets_to_process = [md for md in market_dirs if needs_processing(md)]
        
        if not markets_to_process:
            print("✓ All markets already processed!")
            return
        
        print(f"Found {len(markets_to_process)} market(s) that need processing out of {len(market_dirs)} total markets")
        print("-" * 80)
        
        success_count = 0
        error_count = 0
        
        for i, market_dir in enumerate(markets_to_process, 1):
            market_name = os.path.basename(market_dir)
            print(f"\n[{i}/{len(markets_to_process)}] Processing: {market_name}")
            
            if process_market(market_dir):
                success_count += 1
            else:
                error_count += 1
        
        print("\n" + "=" * 80)
        print(f"Processing complete!")
        print(f"  ✓ Successfully processed: {success_count}")
        if error_count > 0:
            print(f"  ❌ Errors: {error_count}")
        print("=" * 80)

if __name__ == '__main__':
    main()
