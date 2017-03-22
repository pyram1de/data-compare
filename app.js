var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');
var url = 'mongodb://localhost:27017/joinc';
var targetDir = 'E:\\Temp\\fullExtract';
var newLine = '\r\n';
var tab = '\t';
var hasHeader = true;
var outputFile = 'E:\\Temp\\joinersNoExist';
var outputFileSuccess = 'E:\\Temp\\joinersExist';
var outputAll = "E:\\Temp\\outputAll";
var outputOne = "E:\\Temp\\outputOne";
var outputLog = "E:\\Temp\\FilteredExtract\\output.log";
var outputDir = "E:\\Temp\\FilteredExtract\\";
var foundCount = 0;
var headerDone, process;

if(fs.existsSync(outputLog)){ // remove existing file
    fs.unlinkSync(outputLog);
};

run(function* myGenerator(resume){
    var db = yield MongoClient.connect(url, resume);
    var files = fs.readdirSync(targetDir);
    for(var i = 0;i<files.length;i++){
    var file = files[i];
    console.log(file);
    var potentialNumber = file.substr(2,4);
    var inst = parseInt(potentialNumber, 10);
    var instInFile = false;
    if(isNaN(inst)){
        instInFile = true;
    }
    var filename = targetDir + '\\' + file;
    var fileContents = fs.readFileSync(filename, 'utf8');
    var startRow = 0;
    if(hasHeader){
        startRow = 1;
    }
    var fileRows = fileContents.split(newLine);
    var initial = 1;      
    headerDone = false;
    for(var x = 0;x<fileRows.length;x++){
        process = true;
        var row = fileRows[x];
        if(initial&&hasHeader)
        {
            initial = 0;
            continue;
        }
        var rowData = row.split('\t');
        var employer = instInFile ? 'FS' : 'CRBS';
        var nino = rowData[6];
        var dob = rowData[7];
        var surname = rowData[4];
        var firstName = rowData[5];
        var doj = rowData[11];
        var institution = instInFile ? rowData[rowData.length-1] : inst;
        if(nino!==undefined){
            // find joiners....
            var collection = db.collection('hartlinkmembers');
            var ensdd = yield collection.findOne({
                $or: [{"Employer": institution},
                      {"Employer": "MULTIPLE"}],
                "Nino": nino.substr(0,8),
                "Surname": surname.trim(),
                "DOB" : dob,
                "DJS" : doj
            },resume);
            var n = yield collection.findOne({
                "Nino": nino.substr(0,8)
            },resume);
            var en = yield collection.findOne({
                $or: [{"Employer": institution},
                      {"Employer": "MULTIPLE"}],
                "Nino": nino.substr(0,8)
            },resume);
            var esd = yield collection.findOne({
                $or: [{"Employer": institution},
                      {"Employer": "MULTIPLE"}],
                "Surname": surname.trim(),
                "DOB" : dob
            },resume);
            var esdd = yield collection.findOne({
                $or: [{"Employer": institution},
                      {"Employer": "MULTIPLE"}],
                "Surname": surname.trim(),
                "DOB" : dob.trim(),
                "DJS" : doj.trim()
            },resume);
            var sd = yield collection.findOne({
                "Surname": surname.trim(),
                "DOB" : dob
            },resume);
            var fsd = yield collection.findOne({
                "Surname": surname.trim(),
                "First name": firstName.trim(),
                "DOB" : dob
            },resume); 
            var fsds = yield collection.findOne({
                "Surname": firstName.trim(),
                "First name": surname.trim(),
                "DOB" : dob
            },resume);

            switch(nino){ // granular exclusion
                case "JP256044A":
                console.log('excluding member: ' +nino);
                process = false;
                break;
            }

            if(n!==null){ // nino not found.
                process=false;
            }

            if(esd!==null){ // employer surname dob not found
                process=false;
            }

            if(fsds!==null){
                process=false;
            }

            if(process===true){
                // output file.
                if((!headerDone)&&hasHeader){
                    if(fs.existsSync(outputDir+file)){ // remove existing file
                        fs.unlinkSync(outputDir+file);
                    };
                    fs.appendFileSync(outputDir+file, fileRows[0]+ newLine);
                    headerDone=true;
                }
                fs.appendFileSync(outputDir+file, rowData.join(tab) + newLine, 'utf8');
                fs.appendFileSync(outputLog, rowData.join(tab) + newLine,'utf8');
            }

            fs.appendFileSync(outputOne + '.tdf', 
                    institution+tab + 
                    employer+tab+
                    (en?'matched':'')+tab+
                    (n?'matched':'')+tab+
                    (ensdd?'matched':'')+tab+
                    (esd?'matched':'')+tab+
                    (esdd?'matched':'')+tab+
                    (sd?'matched ' + sd["First name"]:'')+tab+
                    (fsd?'matched':'')+tab+
                    (fsds?'matched':'')+tab+
                    rowData.join(tab) + newLine,'utf8');
        }
    }
    };
    db.close();
    console.log('done');
});

function run(generatorFunction){
    var generatorItr = generatorFunction(resume);
    function resume(err, callbackValue){
        if(err){
            console.log(err);
            generatorItr.next(null);
        } else {
            generatorItr.next(callbackValue);
        }
    }
    generatorItr.next();
};
