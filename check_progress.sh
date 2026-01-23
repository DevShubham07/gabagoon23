#!/bin/bash
# Quick script to check processing progress

cd "/Users/shubham.1/Applications/untitled folder/data/2026/01"

TOTAL=$(find . -type d -name "btc-updown-15m-*" | wc -l | tr -d ' ')
PROCESSED=$(find . -name "analysis.txt" -type f | wc -l | tr -d ' ')
NEED_PROCESSING=$(find . -type d -name "btc-updown-15m-*" -exec sh -c 'test -f "$1/user_activity.json" && ! test -f "$1/analysis.txt"' _ {} \; -print | wc -l | tr -d ' ')

echo "📊 Processing Status:"
echo "  Total markets: $TOTAL"
echo "  Processed: $PROCESSED"
echo "  Still need processing: $NEED_PROCESSING"
echo "  Progress: $(( PROCESSED * 100 / TOTAL ))%"

LOG_FILE=$(ls -t "/Users/shubham.1/Applications/untitled folder"/process_markets_*.log 2>/dev/null | head -1)
if [ -n "$LOG_FILE" ] && [ -f "$LOG_FILE" ]; then
    echo ""
    echo "📝 Recent activity:"
    tail -5 "$LOG_FILE" 2>/dev/null | grep -E "Processing:|Completed|Error|ETA" | tail -3
fi
