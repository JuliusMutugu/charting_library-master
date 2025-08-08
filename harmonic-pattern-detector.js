/**
 * Advanced Harmonic Pattern Detection Library
 * Supports: Gartley, Butterfly, Bat, Crab, Shark patterns
 */

class HarmonicPatternDetector {
    constructor() {
        this.patterns = [];
        this.tolerance = 0.05; // 5% tolerance for Fibonacci ratios
        this.minSwingSize = 5;
        
        // Fibonacci ratios
        this.fibRatios = {
            0.236: 0.236,
            0.382: 0.382,
            0.500: 0.500,
            0.618: 0.618,
            0.786: 0.786,
            0.886: 0.886,
            1.000: 1.000,
            1.130: 1.130,
            1.272: 1.272,
            1.414: 1.414,
            1.618: 1.618,
            2.000: 2.000,
            2.236: 2.236,
            2.618: 2.618,
            3.618: 3.618
        };

        // Pattern definitions with strict Fibonacci requirements
        this.patternDefinitions = {
            gartley: {
                name: "Gartley",
                description: "Classic 5-point harmonic pattern",
                ratios: {
                    AB_XA: [0.618], // AB retraces 61.8% of XA
                    BC_AB: [0.382, 0.886], // BC retraces 38.2% to 88.6% of AB
                    CD_BC: [1.272, 1.618], // CD extends 127.2% to 161.8% of BC
                    AD_XA: [0.786] // Final D point at 78.6% of XA
                },
                color: "#FF6B6B",
                priority: 1
            },
            butterfly: {
                name: "Butterfly",
                description: "Extended harmonic pattern with 127%-161.8% extension",
                ratios: {
                    AB_XA: [0.786],
                    BC_AB: [0.382, 0.886],
                    CD_BC: [1.618, 2.618],
                    AD_XA: [1.272, 1.618] // Extension beyond XA
                },
                color: "#4ECDC4",
                priority: 2
            },
            bat: {
                name: "Bat",
                description: "Conservative pattern with 88.6% retracement",
                ratios: {
                    AB_XA: [0.382, 0.500],
                    BC_AB: [0.382, 0.886],
                    CD_BC: [1.618, 2.618],
                    AD_XA: [0.886] // Precise 88.6% level
                },
                color: "#45B7D1",
                priority: 3
            },
            crab: {
                name: "Crab",
                description: "Extreme extension pattern with 161.8% target",
                ratios: {
                    AB_XA: [0.382, 0.618],
                    BC_AB: [0.382, 0.886],
                    CD_BC: [2.236, 3.618],
                    AD_XA: [1.618] // 161.8% extension
                },
                color: "#96CEB4",
                priority: 4
            },
            shark: {
                name: "Shark",
                description: "Alternative pattern with 88.6%-113% completion",
                ratios: {
                    AB_XA: [0.382, 0.618],
                    BC_AB: [1.130, 1.618],
                    CD_BC: [1.618, 2.236],
                    AD_XA: [0.886, 1.130]
                },
                color: "#FFEAA7",
                priority: 5
            },
            cypher: {
                name: "Cypher",
                description: "Scott Carney's Cypher pattern",
                ratios: {
                    AB_XA: [0.382, 0.618],
                    BC_AB: [1.130, 1.414],
                    CD_BC: [1.272, 1.618],
                    AD_XA: [0.786]
                },
                color: "#DDA0DD",
                priority: 6
            }
        };
    }

    /**
     * Calculate Fibonacci ratio between three points
     */
    calculateFibRatio(point1, point2, point3) {
        const move1 = Math.abs(point2 - point1);
        const move2 = Math.abs(point3 - point2);
        return move1 === 0 ? 0 : move2 / move1;
    }

    /**
     * Check if actual ratio matches any target ratios within tolerance
     */
    isValidRatio(actualRatio, targetRatios, customTolerance = null) {
        const tolerance = customTolerance || this.tolerance;
        return targetRatios.some(target => 
            Math.abs(actualRatio - target) <= tolerance
        );
    }

    /**
     * Detect swing points using pivot high/low method
     */
    detectSwingPoints(bars, minSwingSize = null) {
        const swingSize = minSwingSize || this.minSwingSize;
        const swings = [];
        
        if (bars.length < swingSize * 2 + 1) return swings;

        for (let i = swingSize; i < bars.length - swingSize; i++) {
            const currentHigh = bars[i].high;
            const currentLow = bars[i].low;
            
            let isSwingHigh = true;
            let isSwingLow = true;
            
            // Check surrounding bars for pivot high/low
            for (let j = i - swingSize; j <= i + swingSize; j++) {
                if (j !== i) {
                    if (bars[j].high >= currentHigh) isSwingHigh = false;
                    if (bars[j].low <= currentLow) isSwingLow = false;
                }
            }
            
            if (isSwingHigh && !isSwingLow) {
                swings.push({
                    index: i,
                    time: bars[i].time,
                    price: currentHigh,
                    type: 'high',
                    bar: bars[i]
                });
            } else if (isSwingLow && !isSwingHigh) {
                swings.push({
                    index: i,
                    time: bars[i].time,
                    price: currentLow,
                    type: 'low',
                    bar: bars[i]
                });
            }
        }
        
        return swings.sort((a, b) => a.time - b.time);
    }

    /**
     * Validate if 5 points form a valid harmonic pattern
     */
    validateHarmonicPattern(X, A, B, C, D, patternDef) {
        // Calculate all Fibonacci ratios
        const AB_XA = this.calculateFibRatio(X.price, A.price, B.price);
        const BC_AB = this.calculateFibRatio(A.price, B.price, C.price);
        const CD_BC = this.calculateFibRatio(B.price, C.price, D.price);
        const AD_XA = this.calculateFibRatio(X.price, A.price, D.price);

        const ratios = patternDef.ratios;
        
        // Validate all required ratios
        const validAB = this.isValidRatio(AB_XA, ratios.AB_XA);
        const validBC = this.isValidRatio(BC_AB, ratios.BC_AB);
        const validCD = this.isValidRatio(CD_BC, ratios.CD_BC);
        const validAD = this.isValidRatio(AD_XA, ratios.AD_XA);

        return {
            isValid: validAB && validBC && validCD && validAD,
            ratios: { AB_XA, BC_AB, CD_BC, AD_XA },
            validations: { validAB, validBC, validCD, validAD }
        };
    }

    /**
     * Check if swing points form proper alternating pattern
     */
    isValidSwingSequence(X, A, B, C, D) {
        // X and B should be same type (both high or both low)
        // A and C should be same type (opposite of X and B)
        // D should be same type as X and B
        return (X.type === B.type && X.type === D.type && 
                A.type === C.type && A.type !== X.type);
    }

    /**
     * Determine if pattern is bullish or bearish
     */
    getPatternDirection(X, A) {
        return X.type === 'low' && A.type === 'high' ? 'bullish' : 'bearish';
    }

    /**
     * Find all harmonic patterns of specified type
     */
    findHarmonicPatterns(swings, patternType) {
        const patterns = [];
        const patternDef = this.patternDefinitions[patternType];
        
        if (!patternDef || swings.length < 5) return patterns;

        // Search through all possible 5-point combinations
        for (let i = 0; i <= swings.length - 5; i++) {
            for (let j = i + 1; j <= swings.length - 4; j++) {
                for (let k = j + 1; k <= swings.length - 3; k++) {
                    for (let l = k + 1; l <= swings.length - 2; l++) {
                        for (let m = l + 1; m < swings.length; m++) {
                            const X = swings[i];
                            const A = swings[j];
                            const B = swings[k];
                            const C = swings[l];
                            const D = swings[m];

                            // Validate swing sequence
                            if (!this.isValidSwingSequence(X, A, B, C, D)) continue;

                            // Validate pattern ratios
                            const validation = this.validateHarmonicPattern(X, A, B, C, D, patternDef);
                            
                            if (validation.isValid) {
                                patterns.push({
                                    type: patternType,
                                    name: patternDef.name,
                                    points: [X, A, B, C, D],
                                    color: patternDef.color,
                                    direction: this.getPatternDirection(X, A),
                                    ratios: validation.ratios,
                                    confidence: this.calculatePatternConfidence(validation.ratios, patternDef),
                                    timestamp: Date.now(),
                                    priority: patternDef.priority,
                                    description: patternDef.description
                                });
                            }
                        }
                    }
                }
            }
        }
        
        // Sort by confidence and remove duplicates
        return this.filterAndSortPatterns(patterns);
    }

    /**
     * Calculate pattern confidence based on how close ratios are to ideal
     */
    calculatePatternConfidence(actualRatios, patternDef) {
        let totalScore = 0;
        let ratioCount = 0;

        Object.keys(patternDef.ratios).forEach(key => {
            const actualRatio = actualRatios[key];
            const targetRatios = patternDef.ratios[key];
            
            // Find closest target ratio
            const closestTarget = targetRatios.reduce((closest, target) => 
                Math.abs(actualRatio - target) < Math.abs(actualRatio - closest) ? target : closest
            );
            
            // Calculate score (closer to target = higher score)
            const deviation = Math.abs(actualRatio - closestTarget) / closestTarget;
            const score = Math.max(0, 1 - (deviation / this.tolerance));
            
            totalScore += score;
            ratioCount++;
        });

        return ratioCount > 0 ? (totalScore / ratioCount) * 100 : 0;
    }

    /**
     * Filter overlapping patterns and sort by confidence
     */
    filterAndSortPatterns(patterns) {
        // Remove patterns with low confidence
        const filtered = patterns.filter(pattern => pattern.confidence > 70);
        
        // Sort by confidence (highest first)
        return filtered.sort((a, b) => b.confidence - a.confidence);
    }

    /**
     * Scan for all pattern types
     */
    scanAllPatterns(bars, options = {}) {
        const swings = this.detectSwingPoints(bars, options.minSwingSize);
        const allPatterns = [];

        Object.keys(this.patternDefinitions).forEach(patternType => {
            const patterns = this.findHarmonicPatterns(swings, patternType);
            allPatterns.push(...patterns);
        });

        return {
            swings: swings,
            patterns: this.filterAndSortPatterns(allPatterns),
            summary: this.generatePatternSummary(allPatterns)
        };
    }

    /**
     * Generate pattern summary statistics
     */
    generatePatternSummary(patterns) {
        const summary = {
            total: patterns.length,
            byType: {},
            byDirection: { bullish: 0, bearish: 0 },
            avgConfidence: 0
        };

        patterns.forEach(pattern => {
            // Count by type
            summary.byType[pattern.type] = (summary.byType[pattern.type] || 0) + 1;
            
            // Count by direction
            summary.byDirection[pattern.direction]++;
            
            // Add to confidence total
            summary.avgConfidence += pattern.confidence;
        });

        // Calculate average confidence
        if (patterns.length > 0) {
            summary.avgConfidence = summary.avgConfidence / patterns.length;
        }

        return summary;
    }

    /**
     * Get pattern potential reversal zones
     */
    getReversalZones(pattern) {
        const D = pattern.points[4];
        const priceRange = Math.abs(pattern.points[1].price - pattern.points[0].price) * 0.1;
        
        return {
            entry: D.price,
            stopLoss: pattern.direction === 'bullish' ? D.price - priceRange : D.price + priceRange,
            target1: pattern.direction === 'bullish' ? D.price + priceRange : D.price - priceRange,
            target2: pattern.direction === 'bullish' ? D.price + priceRange * 2 : D.price - priceRange * 2
        };
    }
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = HarmonicPatternDetector;
} else if (typeof window !== 'undefined') {
    window.HarmonicPatternDetector = HarmonicPatternDetector;
}
