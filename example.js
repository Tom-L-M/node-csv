const CSVFileParser = require('./lib');
const FILE = '../Dataset/dataset.csv';

(async function () {
    const csv = new CSVFileParser(FILE, { open: true });
    // const iterator = csv.iterator();

    const s1s = Date.now();
    await csv.buildIndex({ printProgress: true });
    const s1e = Date.now();
    console.log('Elapsed time (build index):', s1e - s1s, 'ms');
    console.log(
        'Columns: ' +
            csv.columns +
            ' | Lines: ' +
            csv.lines +
            ' | Cells: ' +
            csv.columns * csv.lines
    );

    // const s2s = Date.now();
    // let counter = 0;
    // for (let line of iterator) {
    //     counter++; // if nothing is done, the loop is optimized
    // }
    // const s2e = Date.now();
    // console.log('Elapsed time (iterator):', s2e - s2s, 'ms');
})();