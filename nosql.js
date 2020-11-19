var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017/";

var fs = require('fs');
var filename = 'movies.dat';
//var filedata = [];

const prompt = require('prompt-sync')({sigint: true});

//Log in to user profile, requires ID
var userlogin = prompt("Enter your user ID: ");
var choice = 0;

function choosePrompt() {
    var promptres = prompt("Nosql Interface (Choose what you want to do): \n 1. Read Rating Data from File(s) \n 2. Add new rating \n 3. Add new movie entry \n 4. Display movies in database \n 5. Display all user ratings \n 6. Exit \n\nEnter Choice:");
    return promptres;
    //console.clear();
}

// Ask for filename from user
function readFile() {
    return prompt("Enter file to read: ");
}

// Read file with data from movietweetings dataset. Each file requires different logic in order to properly convert it into a json object.
function getFileData() {
    filename = readFile();
    var filedata = fs.readFileSync(filename);
    if (filename == "movies.dat") {
        mfstr = parseMovieDataSet(filedata);
        console.log(mfstr);
    }
    if (filename == "users.dat") {
        ufstr = parseUserDataSet(filedata);
        console.log(ufstr);
    }
    if (filename == "ratings.dat") {
        rfstr = parseRatingDataSet(filedata);
        console.log(rfstr);
    }
}

///Takes the data from the movie dataset and converts it into the proper object format
function parseMovieDataSet(data) {
    
    //Split Data into array of strings
    var movstr = data.toString().split(/\r?\n/);

    //Convert movie data from online dataset to proper mognodb format
    //var mformatstr;
    var mformatobj = [];
    var mobj = [];
    for(var i = 0; i <= 10; i++) {
        var mformatstr = '[{ "id":  "' + movstr[i].replace('::','", "name": "').replace(' (','", "year": "').replace(')','').replace('::','", "genre": ["').replace('|','", "') + '"] }]';
        mobj = JSON.parse(mformatstr);
        mformatobj.push(mobj[0]);
    }
    
    return mformatobj;
}

///Takes the data from the user dataset and converts it into the proper object format
function parseUserDataSet(data) {
    
    //Split Data into array of strings
    var usrstr = data.toString().split(/\r?\n/);

    //Convert user data from online dataset to proper mognodb format
    var uformatobj = [];
    var uobj = [];
    for(var i = 0; i <= 10; i++) {
        var uformatstr = '[{ "id":  "' + usrstr[i].replace('::','", "twitterID": "') + '" }]';
        uobj = JSON.parse(uformatstr);
        uformatobj.push(uobj[0]);
    }
    
    return uformatobj;
}

///Takes the data from the ratings dataset and converts it into the proper object format
function parseRatingDataSet(data) {
    
    //Split Data into array of strings
    var ratstr = data.toString().split(/\r?\n/);

    //Convert ratings data from online dataset to proper mognodb format
    var rformatobj = [];
    var robj = [];
    for(var i = 0; i <= 10; i++) {
        var rformatstr = '[{ "userID":  "' + ratstr[i].replace('::','", "movieID": "').replace('::','", "rating": "').replace('::','", "ratingtime": "') + '" }]';
        robj = JSON.parse(rformatstr);
        rformatobj.push(robj[0]);
    }
    
    return rformatobj;
}

//Ask User for movie title and rating in order to add it to the database. If the movie does not currently exist in the database it will ask the user for additional information to make an entry for this movie.
function addRating() {
    var movietitle = prompt("Enter Name of Movie: ");
    var movierating = prompt("Enter Rating: ");
    /*
    MongoClient.connect(url, function(err, db) {
        if (err) throw err;
        
        var dbo = db.db("mdb");
        
        var query = {name: movietitle};
        dbo.collection("Mov").find(query).limit(1).toArray(function(err, res) {
            if (err) throw err;
            console.log(res);
            db.close();
        });
    });
    var newEntryDat = newMovieEntry(movietitle);
    */
    var d = new Date();
    var ratingobj = {userID: userlogin, movieID: movietitle, rating: movierating, ratingtime: d.getTime()};
    
    MongoClient.connect(url, function(err, db) {
        if (err) throw err;
        
        var dbo = db.db("mdb");
        
        dbo.collection("Rat").insertOne(ratingobj, function(err, res) {
            if (err) throw err;
            console.log(res);
            db.close();
        });
    });
    
    //var ratingobj = {_id: "12345", name: movietitle, year: newEntryDat[0], genre: newEntryDat[1]};
    console.log(ratingobj);
    return [movietitle, movierating];
    
}

//Ask user for information on a movie entry in order to add it to the database.
function newMovieEntry(title) {
    console.log("That movie doesn't have an entry in the database, please enter information on the movie in order to add it to the database. \n\n")
    var movieyear = prompt("Enter Movie Year: ");
    var moviegenres = prompt("Enter the Movie Genre's: ");
    return [movieyear, moviegenres];
}

//Directly add a movie entry without rating it
function addMovie() {
    var movietitle = prompt("Enter Name of Movie: ");
    var movieyear = prompt("Enter Movie Year: ");
    var moviegenres = prompt("Enter the Movie Genre's: ");
    
    var movieobj = {_id: "12347", name: movietitle, year: movieyear, genre: moviegenres.split(",")};
    
    MongoClient.connect(url, function(err, db) {
        if (err) throw err;
        
        var dbo = db.db("mdb");
        
        dbo.collection("Mov").insertOne(movieobj, function(err, res) {
            if (err) throw err;
            console.log(res);
            db.close();
        });
    });
    return movieobj;
}

//Display all movies currently in the database
function getMovies() {
    
    var resc = {};
    MongoClient.connect(url, function(err, db) {
        if (err) throw err;
        
        var dbo = db.db("mdb");
        
        var query = {};
        dbo.collection("Mov").find(query, {projection: { _id: 0, name: 1, year: 1, genre: 1}}).toArray(function(err, res) {
            if (err) throw err;
            console.log(res);
            db.close();
        });
    });
    return resc;
}

//Display all ratings from a user
async function getRatingsUser() {
    
    var userid = prompt("Enter User ID: ");
    var resc = {};
    (async () => {
        await MongoClient.connect(url, function(err, db) {
            if (err) throw err;

            var dbo = db.db("mdb");

            var query = {userID: userid};
            const result = await dbo.collection("Rat").find(query, {projection: { userID: 1, movieID: 1, rating: 1, ratingtime: 1}}).toArray(function(err, res) {
                if (err) throw err;
                console.log(res);
                db.close();
            });
        });
    console.log(result.insertedId);
    })();
    return resc;
}

//Dlsplay the average rating for all movies in the database
function getAggregateRatings() {
    await MongoClient.connect(url, function(err, db) {
        if (err) throw err;

        var dbo = db.db("mdb");

        var query = {movieID: movie};
        const result = await dbo.collection("Rat").find(query, {projection: { userID: 1, movieID: 1, rating: 1, ratingtime: 1}}).toArray(function(err, res) {
            if (err) throw err;
            console.log(res);
            db.close();
        });
    });
}

// Query through the database. Depending on genre searched will find movies that the user has not watched that are highly rated by many users with similar taste
function getRecommendedMovieByGenre() {
    
}

// Check user's ratings, tries to query a search that will display movies that are highly rated by users with similar ratings to the user in question.
function getRecommendedMovieByPreference() {
    
}

// Query to display top rated films by genre
function topMoviesByGenre(){
    MongoClient.connect(url, function(err, db) {
        if (err) throw err;
        
        var dbo = db.db("mdb");
        
        var query = {genre: "action"};
        dbo.collection("Mov").find(query, {projection: { _id: 0, name: 1, year: 1, genre: 1}}).toArray(function(err, res) {
            if (err) throw err;
            console.log(res);
            db.close();
        });
    });
    return console.log(res);
}

choice = choosePrompt();

var exitval = 6;
while (choice != exitval) {
    switch(choice) {
        case "1":
            data = getFileData();
            break;
        case "2":
            var ratingres = addRating();
            console.log(ratingres[0] + ratingres[1]);
            break;
        case "3":
            var movieaddres = addMovie();
            console.log(movieaddres);
            break;
        case "4":
            var moviesres = getMovies();
            console.log(moviesres);
            break;
        case "5":
            var ratingsres = getRatingsUser();
            console.log(ratingsres);
            break;
        case "6":
            console.log("Exit");
            break;
    }
    if (choice != exitval) {
        choice = choosePrompt();
    }
}

//mfstr = parseMovieDataSet(data);
//ufstr = parseUserDataSet(data);
//rfstr = parseRatingDataSet(data);

//console.log(rfstr);

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