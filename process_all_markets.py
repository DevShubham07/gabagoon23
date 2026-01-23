#!/usr/bin/env python3
"""
Script to process all markets that have user_activity.json but no analysis.txt
This script can be run periodically to process new markets as they are created.
Includes error handling and progress tracking.
"""

import os
import sys
import time
import traceback
from analyze_user_trades import process_market, needs_processing

def main():
    """Process all markets that need processing."""
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
    start_time = time.time()
    
    for i, market_dir in enumerate(markets_to_process, 1):
        market_name = os.path.basename(market_dir)
        
        # Calculate progress and ETA
        elapsed = time.time() - start_time
        if i > 1:
            avg_time = elapsed / (i - 1)
            remaining = avg_time * (len(markets_to_process) - i)
            eta_min = int(remaining // 60)
            eta_sec = int(remaining % 60)
            print(f"\n[{i}/{len(markets_to_process)}] Processing: {market_name} (ETA: {eta_min}m {eta_sec}s)")
        else:
            print(f"\n[{i}/{len(markets_to_process)}] Processing: {market_name}")
        
        try:
            process_start = time.time()
            if process_market(market_dir):
                success_count += 1
                process_time = time.time() - process_start
                print(f"   ✓ Completed in {process_time:.1f}s")
            else:
                error_count += 1
                print(f"   ❌ Failed to process")
        except KeyboardInterrupt:
            print("\n\n⚠️  Processing interrupted by user")
            print(f"   Processed {success_count} successfully, {error_count} errors")
            break
        except Exception as e:
            error_count += 1
            print(f"   ❌ Error: {str(e)}")
            # Print full traceback to stderr for debugging
            sys.stderr.write(f"\nError processing {market_name}:\n")
            traceback.print_exc(file=sys.stderr)
            # Continue with next market
            continue
        
        # Flush output to ensure logs are written
        sys.stdout.flush()
        sys.stderr.flush()
    
    total_time = time.time() - start_time
    print("\n" + "=" * 80)
    print(f"Processing complete!")
    print(f"  ✓ Successfully processed: {success_count}")
    if error_count > 0:
        print(f"  ❌ Errors: {error_count}")
    print(f"  ⏱️  Total time: {int(total_time // 60)}m {int(total_time % 60)}s")
    print(f"  📊 Average time per market: {total_time / max(success_count + error_count, 1):.1f}s")
    print("=" * 80)

if __name__ == '__main__':
    main()
