function biasedNoiseFloatGenerator(data) {
    const SAMPLE_SIZE = 10;

    if (!Array.isArray(data) || data.length < SAMPLE_SIZE) {
        throw new Error(
            `Data must be an array with at least ${SAMPLE_SIZE} elements.`
        );
    }

    const selectedIndices = new Set();
    const selectedValues = [];

    // Randomly select 10 unique indices
    while (selectedIndices.size < SAMPLE_SIZE) {
        const index = Math.floor(Math.random() * data.length);
        if (!selectedIndices.has(index)) {
            selectedIndices.add(index);
            selectedValues.push(data[index]);
        }
    }

    // Compute mean
    const mean =
        selectedValues.reduce((sum, val) => sum + val, 0) / SAMPLE_SIZE;

    // Apply random noise between ±5% and ±15%
    const noisePercent = Math.random() * 0.1; // 0.05 to 0.15
    const noiseDirection = Math.random() < 0.5 ? -1 : 1;
    const noiseFactor = 1 + noiseDirection * noisePercent;
    const noisyMean = mean * noiseFactor;

    return noisyMean;

    // return {
    //     selected: selectedValues,
    //     mean,
    //     noisePercent: (noiseDirection * noisePercent * 100).toFixed(2) + '%',
    //     noisyMean,
    // };
}

module.exports = biasedNoiseFloatGenerator;
