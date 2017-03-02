var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');

var url = 'mongodb://localhost:27017/joinc';
var targetDir = 'E:\\Temp\\fullExtract';
var newLine = '\r\n';
var hasHeader = true;
var outputFile = 'E:\\Temp\\joinersNoExist';
var outputFileSuccess = 'E:\\Temp\\joinersExist';
var outputAll = "E:\\Temp\\outputAll";
var outputOne = "E:\\Temp\\outputOne";
var foundCount = 0;
var processFile = function(file,db){   
    var potentialNumber = file.substr(2,4);
    var inst = parseInt(potentialNumber, 10);
    var instInFile = false;
    if(isNaN(inst)){
        instInFile = true;
    }
    var filename = targetDir + '\\' + file;
    console.log(filename);
    var fileContents = fs.readFileSync(filename, 'utf8');
    var startRow = 0;
    if(hasHeader){
        startRow = 1;
    }
    var fileRows = fileContents.split(newLine);
    //console.log(fileRows[startRow]);
    var initial = 1;
    fileRows.forEach(function(row){
        if(initial&&hasHeader)
        {
            initial = 0;
            return;
        }
        var rowData = row.split('\t');
        var employer = instInFile ? 'FS' : 'CRBS';
        var nino = rowData[6];
        var dob = rowData[7];
        var surname = rowData[4];
        var doj = rowData[11];
        var institution = instInFile ? rowData[rowData.length-1] : inst;
        if(nino!==undefined){
            //console.log(type + ' ' + nino +  ' ' + dob  +  ' ' + doj  +  ' ' +  institution);        
            // find joiners....
            var collection = db.collection('hartlinkmembers');
            collection.findOne({
                "Employer": institution,
                "Nino": nino.substr(0,8),
                //"Surname": surname,
                "DOB" : dob,
                "DJS" : doj
            },function(err, result){
                if(err){
                    console.log(err);
                    return;
                }
                if(employer==='FS'){
                        //fs.appendFileSync(outputAll + 'FS.log', rowData.join('\t') + newLine,'utf8');                                           
                    } else {
                        //fs.appendFileSync(outputAll + 'CRBS.log', rowData.join('\t') + newLine,'utf8');                                           
                }                                          
                if(result===null){
                    // append to log...
                    //console.log('employer ' + institution + ', nino ' + nino.substr(0,8) +  ', surname ' + surname  +  ', dob ' + dob  +  ', doj ' +  doj);   
                    fs.appendFileSync(outputOne + '.log', institution +'\t' + employer+'\tnot found\t'+rowData.join('\t') + newLine,'utf8');                                           
                    if(employer==='FS'){
                        //fs.appendFileSync(outputFile + 'FS.log', rowData.join('\t') + newLine,'utf8');                                           
                    } else {
                        //fs.appendFileSync(outputFile + 'CRBS.log', rowData.join('\t') + newLine,'utf8');                                           
                    }                   
                } else {
                    // found.....    
                    fs.appendFileSync(outputOne + '.log', institution +'\t' + employer+'\tfound\t'+rowData.join('\t') + newLine,'utf8');                                           
                    foundCount += 1;
                    if(employer==='FS'){
                        //fs.appendFileSync(outputFileSuccess + 'FS.log', rowData.join('\t') + newLine,'utf8');                                           
                    } else {
                        //fs.appendFileSync(outputFileSuccess + 'CRBS.log', rowData.join('\t') + newLine,'utf8');                                           
                    }                    
                }                             
            })        

        }       

    });
    //db.close();
}

MongoClient.connect(url, function(err, db){
    console.log('connected to mongo server');
    
    var files = fs.readdirSync(targetDir);
    files.forEach(function(file){ // foreach is syncronous        
        processFile(file,db);        
    });    
    //console.log('found count...');
    //console.log(foundCount);
    //db.close();
});
