/**
 * Calcula o grau de pertencimento de X usando uma função gaussiana.
 *
 * Fórmula:
 *   x' = exp( - ( (x - μ)^2 ) / (2 σ^2) )
 *
 * @param {number} x - Valor sendo avaliado.
 * @param {number} mean - Média (μ) do atributo.
 * @param {number} std - Desvio-padrão (σ) do atributo.
 * @returns {number} Valor entre 0 e 1 indicando o pertencimento.
 */
function gaussianMembership(x, mean, std) {
    if (std === 0) {
        throw new Error('O desvio-padrão não pode ser zero.');
    }

    const exponent = -Math.pow(x - mean, 2) / (2 * Math.pow(std, 2));
    return Math.exp(exponent);
}

// Exemplo de uso:
const X = 75;
const mean = 80;
const std = 10;

const result = gaussianMembership(X, mean, std);
console.log('Pertencimento gaussiano:', result);
