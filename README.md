# bdp-string-sim
The String Similarity on Flink Project from the Big Data Praktikum @ UNI Leipzig, SS2016
## Run syntax
### Default process
Required parameters:
* `--process default`
* `--inputCsv path/to/concept_attribute.csv`

Description: imports the concept_attribute.csv, filters only label attributes, maps id and value and prints it out

### Create Compare Csv
Required parameters:
* `--process createCompareCsv`
* `--inputCsv path/to/concept_attribute.csv`
* `--outputCsv path/to/output.csv`

Description:
* imports the concept_attribute.csv
* filters only attributes of name 'label'
* maps id and value, e.g. {1,'label',Leipzig,string} is mapped to {1,Leipzig}
* builds the cartesian product with itself
* builds a strict upper triangular matrix by filtering all tuples where id1 < id2
* output as csv

### Calculate Similarity
Required parameters:
* `--process calculateSimilarity`
* `--inputCsv path/to/crossMerged.csv`
* `--outputDir path/to/output/directory`

Optional parameters:
* `--algorithms stringCompare,stringCompareNgram,sortMerge,simmetrics`  
To calculate the string similarity there are 4 different algorithms/techniques. This parameter controls which algorithm(s) will be used. By default, all will be executed.

* `--threshold X.XX`  
Only tuples with a dice similarity >= X.XX will be collected in the result dataset

* `--tokenizeDigits Y`  
Size of an n-gram. Y = 3 by default.

Description:
* imports the crossMerged.csv
* calculates dice similarity by the given algorithms, threshold and digits
* outputs one *.csv file per algorithm with tuple format {(int) id_a, (string)label_a, (int)id_b, (string)label_b, (float)simmilarity_value}

## Data Structure
*concept.csv* columns: entity id, uri, source  
*concept_attributes.csv* columns: entity id, property name, property value, property type  
*linksWithIDs.csv* columns: source entity id, target entity id  
