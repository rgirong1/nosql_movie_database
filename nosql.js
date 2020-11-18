var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017/";

var fs = require('fs');
var filename = 'movies.dat';

const prompt = require('prompt-sync')({sigint: true});

var userid = prompt("Enter your user ID: ");
var choice = 0;
while(choice != 3) {
    choice = prompt("Nosql Interface (Choose what you want to do): \n 1. Read Rating Data from File(s) \n 2. Add new rating \n 3. Exit \n\nEnter Choice:");
    console.clear();
}

// Ask for filename from user
function readFile() {
    return prompt("Enter file to read: ");
}
filename = readFile();
var data = fs.readFileSync(filename);

///Read in file with data
function parseMovieDataSet(data) {
    
    //Split Data into array of strings
    var movstr = data.toString().split(/\r?\n/);

    //Convert movie data from online dataset to proper mognodb format
    //var mformatstr;
    var mformatobj = [];
    var mobj = [];
    for(var i = 0; i <= movstr.length - 1; i++) {
        var mformatstr = '[{ "id":  "' + movstr[i].replace('::','", "name": "').replace(' (','", "year": "').replace(')','').replace('::','", "genre": ["').replace('|','", "') + '"] }]';
        mobj = JSON.parse(mformatstr);
        mformatobj.push(mobj[0]);
    }
    
    return mformatobj;
}

function parseUserDataSet(data) {
    
    //Split Data into array of strings
    var usrstr = data.toString().split(/\r?\n/);

    //Convert movie data from online dataset to proper mognodb format
    var uformatobj = [];
    var uobj = [];
    for(var i = 0; i <= 10; i++) {
        var uformatstr = '[{ "id":  "' + usrstr[i].replace('::','", "twitterID": "') + '" }]';
        uobj = JSON.parse(uformatstr);
        uformatobj.push(uobj[0]);
    }
    
    return uformatobj;
}


function parseRatingDataSet(data) {
    
    //Split Data into array of strings
    var ratstr = data.toString().split(/\r?\n/);

    //Convert movie data from online dataset to proper mognodb format
    var rformatobj = [];
    var robj = [];
    for(var i = 0; i <= 10; i++) {
        var rformatstr = '[{ "userID":  "' + ratstr[i].replace('::','", "movieID": "').replace('::','", "rating": "').replace('::','", "ratingtime": "') + '" }]';
        robj = JSON.parse(rformatstr);
        rformatobj.push(robj[0]);
    }
    
    return rformatobj;
}


//mfstr = parseMovieDataSet(data);
//ufstr = parseUserDataSet(data);
rfstr = parseRatingDataSet(data);

console.log(rfstr);
/*
for(var i = 0; i <= mfstr.length - 1; i++) {
    console.log(mfstr[i]);
}
*/

// Connect to mongodb, add data to collections
MongoClient.connect(url, function(err, db) {
    if (err) throw err;
    
    var dbo = db.db("mdb");
    var movobj = [
        {_id: "12345", name: "mov1", year: "1984", genre: "action"},
        {_id: "12346", name: "mov2", year: "1985", genre: "action"}
    ];
    
    console.log(movobj);
    
    /*
    dbo.collection("Mov").insertMany(mfstr, function(err, res) {
        if (err) throw err;
        console.log(res);
        db.close();
    });
    */
    
    //var grid = new Grid(db, 'fs');
    /*
    dbo.createCollection("Mov", function(err, res) {
        if (err) throw err;
        console.log("Collection created!");
        db.close();
    });
    */
    /*
    dbo.collection("Mov").insertMany(movobj, function(err, res) {
        if (err) throw err;
        //console.log(res.insertedCount + " docs added");
        console.log(res);
        db.close();
    });
    */
    var query = {genre: "action"};
    dbo.collection("Mov").find(query, {projection: { _id: 0, name: 1, year: 1, genre: 1}}).toArray(function(err, res) {
        if (err) throw err;
        //console.log(res.insertedCount + " docs added");
        console.log(res);
        db.close();
    });
});