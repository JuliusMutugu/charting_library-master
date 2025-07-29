# Harmonic Pattern Scanner Documentation

## Overview

This advanced harmonic pattern scanner integrates with TradingView Charting Library to detect and visualize professional-grade harmonic patterns used in technical analysis. The scanner implements Scott Carney's harmonic trading methodology with precise Fibonacci ratio validation.

## Supported Patterns

### 1. Gartley Pattern 🦋
**The Classic 5-Point Pattern**
- **AB Retracement**: 61.8% of XA
- **BC Retracement**: 38.2% to 88.6% of AB  
- **CD Extension**: 127.2% to 161.8% of BC
- **D Completion**: 78.6% of XA
- **Best for**: Conservative entries with high probability

### 2. Bat Pattern 🦇
**Conservative Harmonic Pattern**
- **AB Retracement**: 38.2% to 50% of XA
- **BC Retracement**: 38.2% to 88.6% of AB
- **CD Extension**: 161.8% to 261.8% of BC  
- **D Completion**: 88.6% of XA
- **Best for**: Strong reversal zones with tight stops

### 3. Butterfly Pattern 🦋
**Extended Harmonic Pattern**
- **AB Retracement**: 78.6% of XA
- **BC Retracement**: 38.2% to 88.6% of AB
- **CD Extension**: 161.8% to 261.8% of BC
- **D Completion**: 127.2% to 161.8% of XA (beyond XA)
- **Best for**: Major trend reversals

### 4. Crab Pattern 🦀
**Extreme Extension Pattern**
- **AB Retracement**: 38.2% to 61.8% of XA
- **BC Retracement**: 38.2% to 88.6% of AB
- **CD Extension**: 223.6% to 361.8% of BC
- **D Completion**: 161.8% of XA
- **Best for**: Extreme market conditions

### 5. Shark Pattern 🦈
**Alternative 5-Point Pattern**
- **AB Retracement**: 38.2% to 61.8% of XA
- **BC Extension**: 113% to 161.8% of AB
- **CD Extension**: 161.8% to 223.6% of BC
- **D Completion**: 88.6% to 113% of XA
- **Best for**: Momentum-based reversals

### 6. Cypher Pattern 🔄
**Scott Carney's Advanced Pattern**
- **AB Retracement**: 38.2% to 61.8% of XA
- **BC Extension**: 113% to 141.4% of AB
- **CD Extension**: 127.2% to 161.8% of BC
- **D Completion**: 78.6% of XA
- **Best for**: High-frequency trading setups

## How It Works

### 1. Swing Point Detection
The scanner uses a pivot-based algorithm to identify significant swing highs and lows:
- **Minimum Swing Size**: Configurable lookback period (default: 5 bars)
- **Pivot High**: Price high that's higher than surrounding bars
- **Pivot Low**: Price low that's lower than surrounding bars

### 2. Pattern Validation
Each potential pattern undergoes strict validation:
- **Fibonacci Ratios**: All ratios must fall within tolerance (default: 5%)
- **Swing Sequence**: Points must alternate between highs and lows
- **Time Sequence**: Points must occur in chronological order
- **Confidence Score**: Based on how close ratios are to ideal values

### 3. Pattern Drawing
Valid patterns are drawn with:
- **Connected Lines**: X→A→B→C→D formation
- **Color Coding**: Each pattern type has unique colors
- **Labels**: Pattern name, direction, and confidence
- **Fibonacci Ratios**: Optional ratio annotations
- **Reversal Zones**: Potential entry/exit levels

## Features

### Real-Time Scanning
- **Auto-Scan Mode**: Continuous pattern detection
- **Individual Toggles**: Enable/disable specific patterns
- **Live Updates**: Patterns update as new data arrives
- **Performance Optimized**: Efficient algorithms for real-time use

### Advanced Settings
- **Tolerance Adjustment**: Fine-tune Fibonacci ratio matching
- **Confidence Filtering**: Only show high-probability patterns
- **Swing Size Control**: Adjust sensitivity to market noise
- **Pattern Limits**: Control maximum patterns displayed

### Professional Interface
- **Dark Theme**: Optimized for trading environments
- **Pattern Counters**: Real-time pattern count display
- **Results Panel**: Detailed pattern information
- **Reversal Zones**: Entry, stop-loss, and target levels

## Usage Guide

### Basic Operation
1. **Load the Scanner**: Open `harmonic-scanner-pro.html`
2. **Select Patterns**: Click pattern buttons to enable scanning
3. **Auto-Scan**: Enable automatic continuous scanning
4. **Review Results**: Check the results panel for details

### Pattern Interpretation

#### Bullish Patterns (🟢)
- **Structure**: Low→High→Low→High→Low (X→A→B→C→D)
- **Signal**: Potential upward reversal at D point
- **Entry**: At or near D completion level
- **Stop**: Below D point (risk management)
- **Target**: Previous swing high or Fibonacci extensions

#### Bearish Patterns (🔴)  
- **Structure**: High→Low→High→Low→High (X→A→B→C→D)
- **Signal**: Potential downward reversal at D point
- **Entry**: At or near D completion level
- **Stop**: Above D point (risk management)
- **Target**: Previous swing low or Fibonacci extensions

### Best Practices

#### Pattern Selection
- **Market Conditions**: Choose patterns based on market volatility
- **Timeframes**: Higher timeframes = more reliable patterns
- **Confluence**: Look for multiple patterns confirming same zone
- **Volume**: Validate patterns with volume analysis

#### Risk Management
- **Position Sizing**: Risk only 1-2% per pattern trade
- **Stop Losses**: Always use stops below/above D point
- **Targets**: Take profits at logical resistance/support levels
- **Confirmation**: Wait for price action confirmation at D point

#### Settings Optimization
- **Swing Size**: 
  - Lower values (3-5): More patterns, more noise
  - Higher values (8-15): Fewer patterns, higher quality
- **Tolerance**:
  - Tighter (2-3%): Fewer but more precise patterns
  - Looser (5-8%): More patterns but less precise
- **Confidence**:
  - Higher threshold (80%+): Only best patterns
  - Lower threshold (60%+): More opportunities

## Technical Implementation

### Pattern Detection Algorithm
```javascript
1. Scan price data for swing points
2. Generate all possible 5-point combinations  
3. Validate swing sequence (alternating highs/lows)
4. Calculate Fibonacci ratios for each leg
5. Compare ratios against pattern definitions
6. Score patterns based on ratio precision
7. Filter by confidence threshold
8. Rank by probability and draw top patterns
```

### Fibonacci Calculations
```javascript
// Retracement: How far BC retraces AB
BC_AB_Ratio = |C - B| / |B - A|

// Extension: How far CD extends beyond BC  
CD_BC_Ratio = |D - C| / |C - B|

// Completion: Final D point relative to XA
AD_XA_Ratio = |D - A| / |A - X|
```

### Performance Optimization
- **Efficient Loops**: Optimized nested loops for pattern search
- **Early Termination**: Skip invalid combinations quickly
- **Caching**: Store swing points for reuse
- **Throttling**: Limit scan frequency to prevent overload

## Troubleshooting

### Common Issues

#### "No Patterns Detected"
- **Solution**: Lower confidence threshold or increase tolerance
- **Cause**: Strict settings or insufficient price movement
- **Check**: Ensure enough historical data (50+ bars minimum)

#### "Straight Lines Instead of Patterns"
- **Solution**: Increase minimum swing size setting
- **Cause**: Algorithm detecting false swings in ranging market
- **Check**: Verify chart has clear swing highs and lows

#### "Too Many Patterns"
- **Solution**: Increase confidence threshold or tighten tolerance
- **Cause**: Settings too loose for current market conditions
- **Check**: Focus on highest confidence patterns only

#### "Scanner Not Updating"
- **Solution**: Check auto-scan status and refresh browser
- **Cause**: JavaScript errors or data feed issues
- **Check**: Browser console for error messages

### Performance Issues
- **Slow Scanning**: Reduce number of active patterns
- **Memory Usage**: Clear patterns regularly in long sessions
- **Browser Lag**: Use latest Chrome/Firefox for best performance

## Integration Notes

### TradingView Compatibility
- **Library Version**: Compatible with TradingView Charting Library v29.4.0+
- **Data Feed**: Works with any compatible data feed
- **Symbols**: All asset classes (stocks, forex, crypto, commodities)
- **Timeframes**: All standard timeframes supported

### Customization Options
- **Colors**: Modify pattern colors in `patternDefinitions`
- **Ratios**: Adjust Fibonacci requirements for each pattern
- **UI**: Customize scanner panel styling
- **Features**: Add/remove pattern types as needed

## Advanced Features

### Pattern Confluence
The scanner can detect multiple patterns completing at same zone:
- **Same D Point**: Multiple patterns targeting same level
- **Time Confluence**: Patterns completing simultaneously
- **Probability Boost**: Higher confidence when patterns align

### Automated Alerts
Extend the scanner with custom alerts:
- **Pattern Completion**: Alert when D point is reached
- **High Confidence**: Alert for patterns above 90% confidence
- **Multiple Timeframes**: Scan multiple charts simultaneously

### Backtesting Integration
Export pattern data for backtesting:
- **Pattern History**: Log all detected patterns with outcomes
- **Performance Metrics**: Track pattern success rates
- **Optimization**: Fine-tune settings based on results

## Support and Updates

For technical support or feature requests:
- **Documentation**: Refer to TradingView Charting Library docs
- **Community**: Join harmonic trading forums and Discord
- **Updates**: Check for new pattern types and improvements

## License and Credits

This implementation is based on:
- **Scott Carney**: Original harmonic pattern methodology
- **TradingView**: Charting library and API
- **Fibonacci Analysis**: Mathematical foundations of harmonic trading

---

*Happy trading and may your patterns be profitable! 📈*
