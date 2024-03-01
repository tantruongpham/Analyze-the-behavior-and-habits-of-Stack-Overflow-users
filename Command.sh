# Import Questions File
mongoimport "mongodb://admin:password@mongo:27017/stackoverflow?authSource=admin" -u admin -p password --type csv -d stackoverflow -c questions --headerline --drop /usr/local/share/Questions.csv
# Import Answers File 
mongoimport "mongodb://admin:password@mongo:27017/stackoverflow?authSource=admin" -u admin -p password --type csv -d stackoverflow -c answers --headerline --drop /usr/local/share/Answers.csv

