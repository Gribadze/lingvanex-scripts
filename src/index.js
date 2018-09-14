const fs = require('fs');
const mysql = require('mysql');
const { createLogger, format, transports } = require('winston');

const {
  combine, timestamp, printf, colorize,
} = format;
const myFormat = printf((info) => {
  const message = info instanceof Error ? info.stack : info.message;
  return `${info.timestamp} ${info.level}: ${message}`;
});

const logger = createLogger({
  format: combine(
    colorize(),
    timestamp(),
    myFormat,
  ),
  transports: new transports.Console(),
});

const sourceLang = 'ru';
const targetLang = 'en_GB';
const dictFileName = `dict_${sourceLang}-${targetLang}`;
const connection = mysql.createConnection({
  host: '148.251.176.101',
  user: 'root',
  password: 'dm4cYwYK6EJ25aJN',
  database: 'singleWordsDB',
});

function getSingleWords(lang) {
  return new Promise((resolve, reject) => {
    const words = {};
    logger.info(`fetching words for language "${lang}"...`);
    const query = connection.query(`SELECT * FROM ${lang};`);
    query.on('result', (row) => {
      connection.pause();
      const { uid, word } = row;
      words[uid] = word;
      connection.resume();
    });
    query.on('end', () => {
      logger.info(`${Object.keys(words).length} fetched for "${lang}".`);
      resolve(words);
    });
    query.on('error', (error) => {
      reject(error);
    });
  });
}

function toDictionary(dict, lang, words) {
  const newDict = { ...dict };
  Object.keys(words).forEach((uid) => {
    newDict[uid] = newDict[uid] || {};
    newDict[uid][lang] = words[uid];
  });
  return newDict;
}

function serializeData(data, fileName, dataFormat = 'json') {
  logger.info(`Creating file ${fileName}...`);
  fs.open(fileName, 'w+', 0o666, (err, fd) => {
    const dataToSave = dataFormat === 'json' ? JSON.stringify(data) : data;
    fs.writeSync(fd, dataToSave);
    logger.info('File created.');
  });
}

function parseVocabolary(fileName) {
  return new Promise((resolve, reject) => {
    fs.readFile(fileName, (error, buffer) => {
      if (error) {
        reject(error);
      }
      const data = buffer.toString('utf-8').split('\n');
      const words = {};
      data.forEach((item) => {
        const [key, value] = item.split(' ');
        if (key && value) {
          words[key] = value;
        }
      });
      resolve(words);
    });
  });
}

function createNewDictionary() {
  let dictionary = {};
  return new Promise((resolve, reject) => {
    connection.connect({}, () => {
      logger.info('Connection ready.');
      getSingleWords(sourceLang)
        .then((words) => {
          dictionary = toDictionary(dictionary, sourceLang, words);
        })
        .then(() => getSingleWords(targetLang))
        .then((words) => {
          dictionary = toDictionary(dictionary, targetLang, words);
        })
        .then(() => {
          connection.end(null, () => {
            logger.info('Connection closed.');
          });
          serializeData(dictionary, dictFileName);
          resolve(dictionary);
        })
        .catch(reject);
    });
  });
}

function parseDictionary(size) {
  return new Promise((resolve, reject) => {
    fs.open(dictFileName, 'r', (error, fd) => {
      if (error) {
        reject(error);
      }
      fs.read(fd, Buffer.alloc(size), null, size, null,
        (err, bytesRead, buffer) => {
          if (err) {
            reject(err);
          }
          resolve(JSON.parse(buffer.toString('utf-8')));
        });
    });
  });
}

function getDictionary() {
  return new Promise((resolve, reject) => {
    logger.info('Looking for dictionary file...');
    fs.stat(dictFileName, (error, stats) => {
      if (error && error.code === 'ENOENT') {
        logger.info('Dictionary file not found.');
        resolve(createNewDictionary());
      } else if (error) {
        reject(error);
      } else {
        logger.info(`Using dictionary file "${dictFileName}"`);
        resolve(parseDictionary(stats.size));
      }
    });
  });
}

function exit(code) {
  logger.info(`Process finished with code ${code}`);
  process.exit(code);
}

function normalizeWord(word) {
  return word.toUpperCase().replace(/^[^А-ЯЁA-Z]+|[^А-ЯЁA-Z]+$/g, '');
}

function convertToGIZAFormat(dictionary, sourceVcb, targetVcb) {
  logger.info('Converting dictionary to GIZA format...');
  const gizaDictionary = [];
  Object.keys(sourceVcb).forEach((sourceWordId) => {
    const sourceWord = sourceVcb[sourceWordId];
    logger.info(`Searching translation of word "${sourceWord}"`);
    const dictItem = Object.values(dictionary).find(
      ({ [sourceLang]: word = '' }) => normalizeWord(sourceWord) === normalizeWord(word),
    );
    if (dictItem) {
      const targetWrodId = Object.keys(targetVcb).find(
        id => normalizeWord(dictItem[targetLang]) === normalizeWord(targetVcb[id]),
      );
      if (targetWrodId) {
        logger.info(`Translation found - ${targetVcb[targetWrodId]}`);
        gizaDictionary.push(`${sourceWordId} ${targetWrodId}`);
      } else {
        logger.warn('Translation not found.');
      }
    } else {
      logger.warn(`Word "${sourceWord}" not found in dictionary.`);
    }
  });
  const data = gizaDictionary.join('\n');
  serializeData(data, `${dictFileName}.giza`, 'plain');
}

getDictionary()
  .then(dictionary => Promise.all([
    parseVocabolary(`${sourceLang}.vcb`),
    parseVocabolary(`${targetLang}.vcb`),
  ])
    .then(([sourceVcb, targetVcb]) => convertToGIZAFormat(dictionary, sourceVcb, targetVcb)))
  .catch((error) => {
    logger.error(error);
    exit(1);
  });
