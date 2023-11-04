# Desciption
Ce projet vise a effectuer une analyse des données de la météo

```
echo "# weather" >> README.md
git init
git add README.md
git commit -m "first commit"
git branch -M data-collection
git remote add origin https://github.com/Hammilidi/weather.git
git push -u origin daya-collection
git checkout data-collection
```


worflow1:  nooa------------->             ---------->analyse du changement climatique---->PowerBI
                               ETL(Talend)------>
           open-meteo-------->            ----------->systeme de prediction et d'alerte de catastrophes meteorologiques 
           
           

workflow2: openweather-------->            ------------> analyse energie electrique/meteo--->python Dash
                              kafka--ETL(Spark)------>
           electricitymap----->           ------------>HDFS/construction d'une api
          
               