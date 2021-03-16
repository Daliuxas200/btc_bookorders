const websocket = require('websocket-stream');
const request = require('request');
const fs = require('fs');

let myFile = fs.createWriteStream('myOutput.csv');
setInterval(()=>{
    book && myFile.write(JSON.stringify(book)+'\n');
    console.log('wrote to file');
},1000*60*30)

let depthReadableStream  = websocket('wss://stream.binance.com:9443/ws/btcusdt@depth@1000ms')
let book = {};
let askedForSnapshot = false;

depthReadableStream.on('data', async data => {
    !askedForSnapshot && getBookSnapshot();

    let depth = JSON.parse(data.toString());
    if(book && ((depth.U <= book.lastUpdateId+1) && (depth.u >= book.lastUpdateId+1) || book.lastUpdateId+1 === depth.U)){
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
        book.lastUpdateId = depth.u;
        book.bids = book.bids.filter(b=>parseFloat(b[1])!==0);
        book.asks = book.asks.filter(a=>parseFloat(a[1])!==0);

        let totalAsks = book.asks.reduce((a,c)=>parseFloat(c[1])+a,0).toFixed(2);
        let totalBids = book.bids.reduce((a,c)=>parseFloat(c[1])+a,0).toFixed(2);
        console.log(`Total Bids:${totalBids}, Total Asks:${totalAsks}`);
    }
    
})

function getBookSnapshot(){
    askedForSnapshot = true;
    request('https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000',{ json: true }, (err, res, body) => {
        if (err) { 
            return console.log(err); 
        }
        book = {...body};
    });
}



