const MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017/";

var fs = require('fs');
var filename = 'movies.dat';
//var filedata = [];

const prompt = require('prompt-sync')({sigint: true});

//Log in to user profile, requires ID
var userlogin = prompt("Enter your user ID: ");
var choice = 0;
var fdstr = [];

function choosePrompt() {
    console.log("Nosql Interface (Choose what you want to do): \n 1. Read Rating Data from File(s) \n 2. Add new rating \n 3. Add new movie entry \n 4. Display movies in database \n 5. Display all user ratings \n 6. Show Aggregate Ratings \n 7. Get Movies Recommended by Genre \n 8. Get Movies Recommended from Users Like You \n 9. Exit \n\n");
    var promptres = prompt("Enter Choice:");
    return promptres;
    //console.clear();
}

// Ask for filename from user
function readFile() {
    return prompt("Enter file to read: ");
}

// Read file with data from movietweetings dataset. Each file requires different logic in order to properly convert it into a json object.
async function getFileData(client) {
    filename = readFile();
    var filedata = fs.readFileSync(filename);
    fdcollect = "Movies";
    if (filename == "movies.dat") {
        fdstr = parseMovieDataSet(client, filedata);
        /*
        fdstr = [
            {name: "movietitle", year: "movieyear", genre: "moviegenres"},
            {name: "movietitle", year: "movieyear", genre: "moviegenres"}
        ]
        */
        console.log(fdstr);
        
        const res = await client.db("MovieRatings").collection("Movies").insertMany(fdstr);
        console.log(`${res.insertedCount} movie added with id:`);
        console.log(res.insertedID);
        //console.log(mfstr);
    }
    if (filename == "users.dat") {
        fdstr = parseUserDataSet(client, filedata);

        console.log(fdstr);
        
        const res = await client.db("MovieRatings").collection("Users").insertMany(fdstr);
        console.log(`${res.insertedCount} users added with id:`);
        console.log(res.insertedID);
        //console.log(ufstr);
    }
    if (filename == "ratings.dat") {
        fdstr = parseRatingDataSet(client, filedata);

        console.log(fdstr);
        
        const res = await client.db("MovieRatings").collection("Ratings").insertMany(fdstr);
        console.log(`${res.insertedCount} ratings added with id:`);
        console.log(res.insertedID);
        //console.log(rfstr);
    }
    
}

///Takes the data from the movie dataset and converts it into the proper object format
function parseMovieDataSet(client, data) {
    
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
function parseUserDataSet(client, data) {
    
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
function parseRatingDataSet(client, data) {
    
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
    for (i = 0; i < rformatobj.length; i++){
        rformatobj[i].rating = parseInt(rformatobj[i].rating);
    }
    
    return rformatobj;
}

//Ask User for movie title and rating in order to add it to the database. If the movie does not currently exist in the database it will ask the user for additional information to make an entry for this movie.
async function addRating(client, movietitle, movierating) {
    var movietitle = prompt("Enter Name of Movie: ");
    var movierating = parseInt(prompt("Enter Rating: "));

    var d = new Date();
    var ratingobjmov = {user: userlogin, rating: movierating, ratingtime: d.getTime()};
    var ratingobjusr = {movie: movietitle, rating: movierating, ratingtime: d.getTime()};
    //var ratingobj = {userID: userlogin, movieID: movietitle, rating: movierating, ratingtime: d.getTime()};
    
    const res = await client.db("MovieRatings").collection("Movies").updateOne(
        { name: movietitle },
        { $addToSet: {ratings: ratingobjmov}}
        );
    
    await client.db("MovieRatings").collection("Users").updateOne(
        { id: userlogin },
        { $addToSet: {ratings: ratingobjusr}}
    );
    
    //const res = await client.db("MovieRatings").collection("Ratings").insertOne(ratingobj);
    
    console.log(`${res.insertedCount} rating added with id:`);
    console.log(res.insertedID);
    
    //var ratingobj = {_id: "12345", name: movietitle, year: newEntryDat[0], genre: newEntryDat[1]};
    //console.log(ratingobj);
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
async function addMovie(client) {
    var movietitle = prompt("Enter Name of Movie: ");
    var movieyear = prompt("Enter Movie Year: ");
    var moviegenres = prompt("Enter the Movie Genre's: ");
    
    var movieobj = {name: movietitle, year: movieyear, genre: moviegenres.split(","), ratings: []};
        
    const res = await client.db("MovieRatings").collection("Movies").insertOne(movieobj);
    console.log(`${res.insertedCount} movie added with id:`);
    console.log(res.insertedID);
}

//Display all movies currently in the database
async function getMovies(client) {
    
    //var query = {};
    const cursor = await client.db("MovieRatings").collection("Movies").find({
        
    })
    .sort({name: 1})
    .limit(100);
    
    const res = await cursor.toArray();
    
    if (res.length > 0) {
        console.log(`Movies found in database:`);
        res.forEach((res, i) => {
            //date = new Date(res.)       
            
            console.log();
            console.log(`${i + 1}. name: ${res.name} (${res.year})`);
            console.log(`  _id: ${res._id}`);
            console.log(`  genre's: ${res.genre}`);
            console.log(`  ratings: ${res.ratings}`);
        });
        //console.log(res);
    } else {
        console.log('No Movies Found');
    }
}

//Display all ratings from a user
async function getRatingsUser(client) {
    
    var userid = prompt("Enter User ID: ");
    
    const cursor = await client.db("MovieRatings").collection("Users").find(
        {id: userid}
    )
    .limit(1);
    /*
    const aggCursor = await client.db("MovieRatings").collection("Ratings").aggregate([
        {"$match": {"userID": userid} },
        {"$lookup": {
            "from": "Movies",
            "localField": "movieID",
            "foreignField": "_id",  
            "as": "movfield"
        }},
        {
            $addFields: { movieTitle: "$movfield.name"}
        },
        {"$limit": 10}
    ]); */
    
    const res = await cursor.toArray();
    //console.log(res);
    
    if (res.length > 0) {
        console.log(`Ratings from user ${res.twitterID}`);
        res.forEach((res, i) => {
            //date = new Date(res.)       
            
            for(i = 0; i < res.ratings.length; i++) {
                console.log();
                //console.log(res.ratings[i]);
                console.log(`${i + 1}. ${res.ratings[i].movie} rating: ${res.ratings[i].rating}/10`);
                //console.log(`  _id: ${res._id}`);
                console.log(`  rating time: ${res.ratings[i].ratingtime}`);
            }
        });
        //console.log(res);
    } else {
        console.log(`No Movie Ratings Found from user ${userid}`);
    }
    
}

//Dlsplay the average rating for all movies in the database
async function getAggregateRatings(client) {
    
    /*
    const aggCursor = await client.db("MovieRatings").collection("Movies").aggregate([
        {$group: {
            _id: "$movieID",
            avgRating: {$avg: "$rating"},
            //ratingtime: "$ratingtime"
        }
        }
    ]);
    */
    const aggCursor = await client.db("MovieRatings").collection("Movies").aggregate([
        {"$unwind": "$ratings"},
        {$group: {
            _id: "$name",
            avgRating: {$avg: "$ratings.rating"},
            //ratingtime: "$ratingtime"
        }
        }
    ]);
    
    const res = await aggCursor.toArray();
    
    //console.log(res);
    
    if (res.length > 0) {
        console.log(`Ratings from database:`);
        res.forEach((res, i) => {
            //date = new Date(res.)       
            
            console.log();
            console.log(`${i + 1}. ${res._id} rating: ${res.avgRating}`);
        });
    } else {
        console.log('No Ratings Found');
    }
}

// Query through the database. Depending on genre searched will find movies that the user has not rated that are highly rated.
async function getRecommendedMovieByGenre(client) {
    var genreSearch = prompt("Enter Genre to get Recommendations from: ");
    
    const aggCursor = await client.db("MovieRatings").collection("Movies").aggregate([
        {"$unwind": "$genre"},
        {"$match": {genre: genreSearch}},
        {"$unwind": "$ratings"},
        {$group: {
            _id: "$name",
            avgRating: {$avg: "$ratings.rating"},
            //ratingtime: "$ratingtime"
        }}
        /*
        {"$lookup": {
            "from": "Movies",
            "localField": "movieID",
            "foreignField": "_id",  
            "as": "movfield"
        }},
        {"$addFields": {}}
        */
        /*
        {"$unwind": "$movfield" },
        {$match: { movfield: {$elemMatch: {genreSearch}}} },
        
        {$group: {
            _id: "$movieID",
            avgRating: {$avg: "$rating"}
            //ratingtime: "$ratingtime"
        }}*/
        
    ]);
    
    const res = await aggCursor.toArray();
    
    console.log(res);
    
    if (res.length > 0) {
        console.log(`Ratings from database:`);
        res.forEach((res, i) => {
            //date = new Date(res.)       
            
            console.log();
            console.log(`${i + 1}. ${res._id} rating: ${res.avgRating}`);
        });
    } else {
        console.log('No Ratings Found');
    }
}

// Check user's ratings, tries to query a search that will display movies that are highly rated by users with similar ratings to the user in question.
async function getRecommendedMovieByPreference(client) {
    
}
/*
// Query to display top rated films by genre
function topMoviesByGenre(client){
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
*/
async function userGUI(client) {
    choice = choosePrompt();

    var exitval = 9;
    while (choice != exitval) {
        switch(choice) {
            case "1":
                await getFileData(client);
                break;
            case "2":
                var ratingres = await addRating(client);
                console.log(ratingres[0] + ratingres[1]);
                break;
            case "3":
                await addMovie(client);
                break;
            case "4":
                await getMovies(client);
                break;
            case "5":
                await getRatingsUser(client);
                break;
            case "6":
                await getAggregateRatings(client);
                break;
            case "7":
                await getRecommendedMovieByGenre(client);
                break;
            case "8":
                await getRecommendedMovieByPreference(client);
                break;
            case "9":
                console.log("Exit");
                break;
        }
        if (choice != exitval) {
            choice = choosePrompt();
        }
    }
}

async function listDatabases(client) {
    const databasesList = await client.db().admin().listDatabases();
    
    console.log("Databases: ");
    databasesList.databases.forEach(db => console.log(` - ${db.name}`));
    
}

async function main() {
    const uri = "mongodb+srv://rgirong1:rgirong1mongodbbing@cluster0.oelf3.mongodb.net/<dbname>?retryWrites=true&w=majority";
    const client = new MongoClient(uri, {useNewUrlParser: true, useUnifiedTopology: true});
    try {
        await client.connect();
        await userGUI(client);
        //await addMovie(client);
    } catch (err) {
        console.error(err);
    } finally {
        await client.close();
    }
}

main().catch(console.error);