const websocket = require('websocket-stream');
const request = require('request');
// const fs = require('fs');
const moment = require('moment');

const config = require('./config.js')();
const { Client } = require('pg');
const db = new Client(config['db']);
db.connect();

var args = process.argv.slice(2);

//Settings for running
let emitPeriodMinutes = parseInt(args[1]);
let maxRetry = 10;
let retryAfter = 5000; // ms

let retryCount = 0;

symbol = args[0];
function start(){

    let depthReadableStream  = websocket(`wss://stream.binance.com:9443/ws/${symbol.toLowerCase()}@depth@1000ms`)
    let book = {};
    let askedForSnapshot = false;

    // writing data to file at set intervals
    let writeIntervalID = setInterval(()=>{
        // book && myFile.write(JSON.stringify(book)+'\n');
        book && db.query('INSERT INTO binance_order_book (symbol, response) VALUES ($1,$2)', [symbol, JSON.stringify(book)]);
        console.log(`Wrote to DB on: ${moment().format()} , Data collection duration: ${moment(book.lastTimestamp).diff(moment(book.firstTimestamp),'minutes')} minutes`);
    },1000*60*emitPeriodMinutes);

    depthReadableStream.on('data', data => {
        // if succesful data, reset Retries;
        retryCount = 0;

        // Check if request for base Book snapchot has been sent, if not sent it
        !askedForSnapshot && getBookSnapshot();

        // Parse depth diff data from stream
        let depth = JSON.parse(data.toString());

        // Check if a book object has been initiated with data and for data continuity based on first/last update ID in the diffs.
        if(book && ((depth.U <= book.lastUpdateId+1) && (depth.u >= book.lastUpdateId+1) || book.lastUpdateId+1 === depth.U)){

            // loop the diffs to update/add any entries in the current version our book object
            for(let askDiff of depth.a){
                let found = false;
                book.asks = book.asks.map(a => {
                    if(a[0] === askDiff[0]){
                        found = true;
                        return askDiff;
                    } else {
                        return a;
                    }
                })
                if(!found && parseFloat(askDiff[1])!==0) book.asks.push(askDiff)
            }
            for(let bidDiff of depth.b){
                let found = false;
                book.bids = book.bids.map(b=>{
                    if(b[0] === bidDiff[0]){
                        found = true;
                        return bidDiff;
                    } else {
                        return b;
                    }
                })
                if(!found && parseFloat(bidDiff[1])!==0) book.bids.push(bidDiff)
            }
            // filter out any values with 0.00 quantity
            book.bids = book.bids.filter(b=>parseFloat(b[1])!==0);
            book.asks = book.asks.filter(a=>parseFloat(a[1])!==0);

            // update the book lastUpadteId to match that of the latest Diff, also update the timestamp of latest data entry.
            book.lastUpdateId = depth.u;
            book.lastTimestamp = moment();

            // calculating totals for Bids and Asks in BTC, spam console.
            let totalAsks = book.asks.reduce((a,c)=>parseFloat(c[1])+a,0).toFixed(2);
            let totalBids = book.bids.reduce((a,c)=>parseFloat(c[1])+a,0).toFixed(2);
            console.log(`Total Bids:${totalBids}, Total Asks:${totalAsks}`);

        // Else if book exists but our updates have skipped over our books timeline, reset the process
        } else if(book && depth.U > book.lastUpdateId+1){
            askedForSnapshot = false;
            book = {};
        }
        
    })

    depthReadableStream.on('error', err => {
        console.log(err.name, err.message);
        setTimeout(()=>{
            if(retryCount<maxRetry){
                depthReadableStream.destroy();
                retryCount++;
                clearInterval(writeIntervalID)
                start();
            } else {
                console.log('Max retries. Stopping :(')
                process.exit();
            }
        },retryAfter)
    })

    // Function for getting the snapshot of the Book 
    function getBookSnapshot(){
        askedForSnapshot = true;
        request(`https://api.binance.com/api/v3/depth?symbol=${symbol.toUpperCase()}&limit=1000`,{ json: true }, (err, res, body) => {
            if (err) { 
                return console.log(err); 
            }
            book = {...body};
            book.firstTimestamp = moment();
            book.lastTimestamp = moment();
        });
    }

}

start();
