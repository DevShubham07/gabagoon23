#!/usr/bin/env python3
"""
Script to find and fix markets that have user_activity.json but are missing analysis.txt or charts.
"""

import os
import sys
from analyze_user_trades import process_market

def find_incomplete_markets(base_dir):
    """Find markets that need processing."""
    all_items = os.listdir(base_dir)
    market_dirs = [os.path.join(base_dir, item) for item in all_items 
                  if os.path.isdir(os.path.join(base_dir, item)) and 
                  item.startswith('btc-updown-15m-')]
    
    incomplete = []
    for md in market_dirs:
        market_name = os.path.basename(md)
        has_user_activity = os.path.exists(os.path.join(md, 'user_activity.json'))
        has_analysis = os.path.exists(os.path.join(md, 'analysis.txt'))
        
        # Check for charts (should have 5 PNG files)
        chart_files = [f for f in os.listdir(md) if f.endswith('.png') and f.startswith(('1_', '2_', '3_', '4_', '5_'))]
        has_charts = len(chart_files) >= 5
        
        if has_user_activity and (not has_analysis or not has_charts):
            incomplete.append((md, market_name, not has_analysis, not has_charts, len(chart_files)))
    
    return incomplete

def main():
    """Process incomplete markets."""
    base_dir = '/Users/shubham.1/Applications/untitled folder/data/2026/01'
    
    if not os.path.exists(base_dir):
        print(f"Error: Base directory not found: {base_dir}")
        return
    
    incomplete = find_incomplete_markets(base_dir)
    
    if not incomplete:
        print("✓ All markets are complete!")
        return
    
    print(f"Found {len(incomplete)} incomplete market(s)")
    print("-" * 80)
    
    # Group by issue type
    missing_analysis = [m for m in incomplete if m[2]]
    missing_charts = [m for m in incomplete if m[3] and not m[2]]
    
    print(f"  Missing analysis.txt: {len(missing_analysis)}")
    print(f"  Missing charts (but has analysis): {len(missing_charts)}")
    print()
    
    success_count = 0
    error_count = 0
    
    for i, (market_dir, market_name, missing_analysis_flag, missing_charts_flag, chart_count) in enumerate(incomplete, 1):
        issue = []
        if missing_analysis_flag:
            issue.append("analysis")
        if missing_charts_flag:
            issue.append(f"{chart_count}/5 charts")
        
        print(f"[{i}/{len(incomplete)}] Processing: {market_name} (missing: {', '.join(issue)})")
        
        try:
            if process_market(market_dir):
                success_count += 1
                print(f"   ✓ Completed")
            else:
                error_count += 1
                print(f"   ❌ Failed")
        except KeyboardInterrupt:
            print("\n\n⚠️  Processing interrupted by user")
            break
        except Exception as e:
            error_count += 1
            print(f"   ❌ Error: {str(e)}")
            import traceback
            traceback.print_exc()
            continue
        
        sys.stdout.flush()
        sys.stderr.flush()
    
    print("\n" + "=" * 80)
    print(f"Processing complete!")
    print(f"  ✓ Successfully processed: {success_count}")
    if error_count > 0:
        print(f"  ❌ Errors: {error_count}")
    print("=" * 80)

if __name__ == '__main__':
    main()
