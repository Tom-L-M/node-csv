const fs = require('node:fs');
const readline = require('node:readline');

/**
 * Implements a progress bar for the console.
 *
 * @class ProgressBar
 *
 * @constructor
 * @param  {number} total The size of the progress bar (the number of times it can be updated before conclusion).
 * @param  {string} str_left The char to use as the left char (each stepped segment - this is usually not changed).
 * @param  {string} str_right The char to use as the right char (each empty segment - this is usually not changed).
 *
 * @example <caption> A progress bar that updates every second: </caption>
 *  let progress = new ProgressBar(10);
 *  let s = setInterval(()=>{
 *      if (progress.ended) clear();
 *      progress.update();
 *  }, 1000);
 *  const clear = () => clearInterval(s);
 */
class ProgressBar {
    constructor(
        total,
        { unit_posfix = '', str_left = '■', str_right = ' ' } = {}
    ) {
        this.str_left = str_left;
        this.str_right = str_right;
        this.unit_posfix = unit_posfix;
        this.total = total;
        this.current = 0;
        this.strtotal = 60; //progress bar width.
        this.ended = false;
    }

    resize(newsize) {
        this.reset();
        this.total = newsize;
    }
    /**
     * Updates the progress bar (moves one step right).
     * @since 1.2.22
     * @param  {number} current The position to jump the progress to. Only used if you want a non-linear progress movement.
     * @return {ProgressBar}
     */
    update(current) {
        this.current++;
        if (current) this.current = current;
        let dots = this.str_left.repeat(
            parseInt(((this.current % this.total) / this.total) * this.strtotal)
        );
        let left =
            this.strtotal -
            parseInt(
                ((this.current % this.total) / this.total) * this.strtotal
            );
        let empty = this.str_right.repeat(left);
        process.stdout.write(
            `\r[${dots}${empty}] ` +
                `${parseInt((this.current / this.total) * 100)}%  ` +
                `(${this.current}/${this.total} ${this.unit_posfix})`
        );
        if (this.total <= this.current) {
            process.stdout.write(
                `\r[${this.str_left.repeat(this.strtotal)}] 100%  ` +
                    `(${this.current}/${this.total} ${this.unit_posfix})\n`
            );
        }
        return this;
    }

    /**
     * Clears the progress bar.
     * @since 1.2.22
     * @return {undefined}
     */
    reset() {
        this.current = 0;
        this.ended = false;
    }
}

class CSVObjectLine {
    constructor({ index = 0, line = null, cells = [] } = {}) {
        this.index = index;
        this.line = line;
        this.cells = cells;
        this.hasMissingCells = false;
        this.hasExcessCells = false;
        this.fields = { _unnamed: [] };
    }
}

class CSVFileParser {
    #header;
    #filename;
    #index_pool;
    #lines;
    #columns;
    #size;
    #input_stream;
    #reading_handle;
    #iterator_stream;
    #iterator_object;
    #iterator_function;
    #is_open;
    #is_indexed;
    #is_iterator_active;
    #reading_buffer;
    #line_divisor;

    /**
     * @param {string} filename
     * @param {Object} [param1={}]
     * @param {boolean} [param1.open=false] - If set to true, auto-opens the file during class instance creation
     * @example
     *  // Crate and open the file handler for traversal and create the handles
     *  const csv = new CSVFileParser('somefile.csv', { open: true });
     *  // If 'open: true' is not specified, the file must be opened
     *  //  manually later, with 'csv.open()';
     *
     *  // Build the file indexes, for accessing individual lines:
     *  await csv.buildIndex();
     *
     *  // Get individual lines of the file:
     *  const header = csv.header; // Array<string>
     *  const line_1 = csv.getLine(1); // {CSVObjectLine}
     *  const line_1000 = csv.getLine(1000); // {CSVObjectLine}
     *
     *  // Or, get an iterator for the file:
     *  const iterator = csv.iterator();
     *  for await (let line of iterator) {
     *      console.log(line.index, line.fields); // {CSVObjectLine}
     *  }
     *
     *  // Rewind the iterators and handles to start reading again
     *  csv.rewind();
     *
     *
     */
    constructor(filename, { open = false } = {}) {
        this.#filename = filename;
        this.#index_pool = [];
        this.#header = null;
        this.#lines = 0;
        this.#columns = 0;
        this.#size = 0;

        this.#input_stream = null;
        this.#reading_handle = null;
        this.#iterator_stream = null;
        this.#iterator_object = null;
        this.#iterator_function = null;

        this.#is_open = false;
        this.#is_indexed = false;
        this.#is_iterator_active = false;
        this.#reading_buffer = null;

        this.#line_divisor = '\n';
        if (Boolean(open)) this.open();
    }

    #throwMissingIndexingError(originFunction, message) {
        throw new Error(
            `[${this.constructor.name}.${originFunction}] ${message}. ` +
                `Use <${this.constructor.name}.buildIndex()> first, ` +
                `or use an iterator object <${this.constructor.name}.iterator()>.`
        );
    }

    get filename() {
        return this.#filename;
    }

    get header() {
        if (!this.isIndexed)
            this.#throwMissingIndexingError(
                'header',
                'Cannot fetch header, file is not indexed'
            );
        return this.#header;
    }

    get lines() {
        if (!this.isIndexed)
            this.#throwMissingIndexingError(
                'lines',
                'Cannot fetch header, file is not indexed'
            );
        return this.#lines;
    }

    get columns() {
        if (!this.isIndexed)
            this.#throwMissingIndexingError(
                'columns',
                'Cannot fetch header, file is not indexed'
            );
        return this.#columns;
    }

    get size() {
        if (!this.isIndexed)
            this.#throwMissingIndexingError(
                'size',
                'Cannot fetch header, file is not indexed'
            );
        return this.#size;
    }

    get isOpen() {
        return this.#is_open;
    }

    get isIndexed() {
        return this.#is_indexed;
    }

    /**
     * Parses a string as an entry of a CSV file, and returns an array with each cell
     * Empty cells are returned as 'null'.
     * @param {string} line
     * @returns {Array<string>}
     */
    #splitCSVLine(line) {
        //TODO - implement this properly, to handle ["] and [']
        return line.split(',');
    }

    /**
     * Reads the specified number of bytes from the CSV file at the specified position
     * @param {number} index - The offset to read bytes from
     * @param {number} length - The number of bytes to read
     * @returns {string}
     */
    #readAtIndex(index, length) {
        return new Promise((resolve, reject) => {
            const buffer = this.#reading_buffer || Buffer.alloc(length);
            fs.read(
                this.#reading_handle,
                buffer,
                0,
                length,
                index,
                (err, bytesRead, buffer) => {
                    if (err) reject({ error: err, result: null });
                    resolve({
                        error: null,
                        result: buffer
                            .slice(0, length)
                            .toString('utf-8')
                            .trim(),
                    });
                }
            );
        });
    }

    /**
     * Parses and wraps a line string as a CSVObjectLine, with included properties and values
     * @param {string} line - The line string
     * @param {number} index - The index of the line in the entire CSV dataset
     * @param {Object} [param2={}]
     * @param {null} [param2.header=null] - An optional header to use.
     * Used only if the user did not call ".buildIndex()" before.
     * @returns {CSVObjectLine}
     */
    #buildLineObject(line, index, { header = null } = {}) {
        // If it is not an array, convert to one
        if (typeof header === 'string') header = this.#splitCSVLine(header);

        const _header = header || this.#header;

        const cells = this.#splitCSVLine(line);
        const result = new CSVObjectLine({ index, line, cells });

        // If there are cells without a defined header name
        if (cells.length > _header.length) {
            result.hasExcessCells = true;

            for (let i = 0; i < cells.length; i++) {
                const col = _header[i];
                const cell = cells[i] || null;

                if (!col) {
                    result.fields._unnamed.push(cell);
                    continue;
                }

                if (typeof result.fields[col] === 'string') {
                    result.fields[col] = [result.fields[col], cell];
                    continue;
                }

                if (Array.isArray(result.fields[col])) {
                    result.fields[col].push(cell);
                    continue;
                }

                result.fields[col] = cell;
            }
        }
        //
        else {
            for (let i = 0; i < _header.length; i++) {
                const col = _header[i];
                const cell = cells[i] || null;

                if (typeof result.fields[col] === 'string') {
                    result.fields[col] = [result.fields[col], cell];
                    continue;
                }

                if (Array.isArray(result.fields[col])) {
                    result.fields[col].push(cell);
                    continue;
                }

                result.fields[col] = cell;
            }
        }

        return result;
    }

    /**
     * Opens the file for processing and create the necessary handles and streams
     * @returns {CSVFileParser}
     */
    open() {
        if (this.#is_open)
            throw new Error(
                `[${this.constructor.name}.open()] ` +
                    `Cannot open file '${this.#filename}': ` +
                    `file is already open. Use <${this.constructor.name}.close()> first.`
            );

        this.#input_stream = fs.createReadStream(this.#filename, {
            encoding: 'utf-8',
            autoClose: false,
        });

        this.#iterator_stream = readline.createInterface({
            input: this.#input_stream,
            crlfDelay: Infinity,
        });

        if (!this.#reading_handle)
            this.#reading_handle = fs.openSync(this.#filename, 'r');

        this.#is_open = true;

        if (!this.#line_divisor) {
            const tempbuffer = Buffer.alloc(1024 * 8);
            fs.readSync(
                this.#reading_handle,
                tempbuffer,
                0,
                tempbuffer.length,
                0
            );
            this.#line_divisor = tempbuffer.toString('utf-8').includes('\r\n')
                ? '\r\n'
                : '\n';
        }

        return this;
    }

    /**
     * Opens the file and remove dangling handles and streams
     * @param {Object} [param0={}]
     * @param {boolean} [param0.preserveFileHandle=false]
     * If specified, does not close the FS file handle.
     * This is used internally and not destinated for direct use.
     * Use only if intending to open and close the file multiple times.
     * @returns {CSVFileParser}
     */
    close({ preserveFileHandle = false } = {}) {
        if (!this.#is_open)
            throw new Error(
                `[${this.constructor.name}.close()] ` +
                    `Cannot close file '${this.#filename}': ` +
                    `file is not open. Use <${this.constructor.name}.open()> first.`
            );

        this.#input_stream.close();
        this.#iterator_stream.close();

        this.#input_stream = null;
        this.#iterator_stream = null;

        if (!preserveFileHandle) {
            fs.closeSync(this.#reading_handle);
            this.#reading_handle = null;
        }

        this.#is_open = false;

        return this;
    }

    /**
     * Traverses the entire CSV dataset and generate an index of entry offsets and
     * entry lengths, to auxiliate in the process of fetching single lines later.
     * @param {Object} [param0={}]
     * @param {number} [param0.max=-1] If a value is specified, parses only the first X lines.
     * @returns {Promise<CSVFileParser>}
     */
    async buildIndex({ max = -1, printProgress = false } = {}) {
        if (!this.#is_open)
            throw new Error(
                `[${this.constructor.name}.buildIndex()] ` +
                    `Cannot build index of file '${this.#filename}': ` +
                    `file is not open. Use <${this.constructor.name}.open()> first.`
            );

        this.#lines = 0;
        this.#size = 0;

        let maxLength = 0;

        const totalsize = Boolean(printProgress)
            ? fs.fstatSync(this.#reading_handle).size
            : null;
        const progress = Boolean(printProgress)
            ? new ProgressBar(
                  ...(max > 0
                      ? [max, { unit_posfix: 'lines' }]
                      : [totalsize, { unit_posfix: 'bytes' }])
              ).update(1)
            : null;

        for await (let line of this.#iterator_stream) {
            if (this.#lines === 0) {
                this.#lines++;
                this.#size += line.length + this.#line_divisor.length;
                this.#header = this.#splitCSVLine(line);
                this.#columns = this.#header.length;
                continue;
            }

            if (Boolean(printProgress))
                progress.update(max > 0 ? this.#lines : this.#size);

            if (max >= 0 && this.#lines >= max) break;

            this.#index_pool.push([
                this.#size,
                line.length + this.#line_divisor.length,
            ]);

            this.#lines++;
            this.#size += line.length + this.#line_divisor.length;
            if (line.length >= maxLength) maxLength = line.length;
        }

        this.#reading_buffer = Buffer.alloc(maxLength);

        // Reset and close streams
        this.close({ preserveFileHandle: true });

        // Reopen streams with to reset iterator
        this.open();

        this.#is_indexed = true;

        return this;
    }

    /**
     * Resets the streams and rewinds the process, to start reading the content again
     * @returns {CSVFileParser}
     */
    rewind() {
        if (!this.#is_open)
            throw new Error(
                `[${this.constructor.name}.rewind()] ` +
                    `Cannot rewind file '${this.#filename}': ` +
                    `file is not open. Use <${this.constructor.name}.open()> first.`
            );
        this.close({ preserveFileHandle: true });
        this.open();
        return this;
    }

    /**
     * Creates (or returns, if already created) a singleton async iterator for the CSV
     * content in the file.
     * @returns {AsyncGenerator<CSVObjectLine, void, unknown>}
     * @example
     *  const csv = new CSVFileParser('somefile.csv');
     *  const iterator = csv.iterator();
     * // Method 1:
     *  for await (let line of iterator) {
     *      console.log(line) // {CSVObjectLine}
     *  }
     * // Method 2:
     * console.log(iterator.next()) // { value: CSVObjectLine, done: false }
     * ...
     * console.log(iterator.next()) // { value: CSVObjectLine, done: true }
     *
     */
    iterator() {
        if (!this.#is_open)
            throw new Error(
                `[${this.constructor.name}.iterator()] ` +
                    `Cannot get iterator for file '${this.#filename}': ` +
                    `file is not open. Use <${this.constructor.name}.open()> first.`
            );

        // Only one iterator may exist at a time
        if (this.#is_iterator_active) {
            return this.#iterator_object;
        }

        const scopeHeader = this.#header;
        const scopeIteratorStream = this.#iterator_stream;
        const scopeLineObjectBuilder = this.#buildLineObject;

        const generatorConstructor = async function* csvAsyncIteratorWrapper() {
            let index = 0;
            let header = scopeHeader || null;
            for await (const line of scopeIteratorStream) {
                index++;
                // if is first line, ignore (it is the header)
                if (index === 1) {
                    if (!header) header = line;
                    continue;
                }
                yield scopeLineObjectBuilder(line, index, { header });
            }
        };

        this.#iterator_function = generatorConstructor;
        this.#is_iterator_active = true;
        this.#iterator_object = this.#iterator_function();

        return this.#iterator_object;
    }

    /**
     * Fetch a line from the CSV content at a specific line number
     * (Line numbers start at 1, 0 is the header).
     * @param {number} index
     * @returns {CSVObjectLine}
     */
    async getLine(index) {
        if (!this.#is_open)
            throw new Error(
                `[${this.constructor.name}.getLine()] ` +
                    `Cannot fetch indexed line for file '${this.#filename}': ` +
                    `file is not open. Use <${this.constructor.name}.open()> first.`
            );
        if (!this.#is_indexed || !this.#index_pool.length)
            throw new Error(
                `[${this.constructor.name}.getLine()] ` +
                    `Cannot fetch indexed line for file '${this.#filename}': ` +
                    `file is not indexed. Use <${this.constructor.name}.buildIndex()> first.`
            );

        if (
            !index ||
            typeof index !== 'number' ||
            index > this.#lines ||
            index <= 0 ||
            !this.#index_pool[index][1]
        )
            throw new Error(
                `[${this.constructor.name}.getLine()] ` +
                    `Cannot fetch indexed line '${index}' for file ` +
                    `'${this.#filename}': line index out of range. ` +
                    `Expected an index between 1 and ${this.#lines}`
            );

        // TODO - 'index' must be a byte offset, not a line offset
        const line = await this.#readAtIndex(
            this.#index_pool[index][0],
            this.#index_pool[index][1]
        );

        if (line.error) {
            throw new Error(
                `[${this.constructor.name}.getLine()] ` +
                    `Cannot fetch indexed line '${index}' for file ` +
                    `'${this.#filename}': error during read. ${
                        line.error.message
                    }`
            );
        }
        const result = this.#buildLineObject(line.result, index);

        return result;
    }
}

module.exports = CSVFileParser;
