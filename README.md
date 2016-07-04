# bdp-string-sim
The String Similarity on Flink Project from the Big Data Praktikum @ UNI Leipzig, SS2016
## Run syntax
Please use the following syntax to start the application

batch_process arg1 arg2 ...

## Batch processes
### Default process
* Name: default
* arg1: path/to/concept_attribute.csv

Description: imports the concept_attribute.csv, filters only label attributes, maps id and value and prints it out

### Create Compare Csv
* Name: createCompareCsv
* arg1: path/to/concept_attribute.csv
* arg2: path/to/output.csv

Description:
* imports the concept_attribute.csv
* filters only attributes of name 'label'
* maps id and value, e.g. {1,'label',Leipzig,string} is mapped to {1,Leipzig}
* builds the cartesian product with itself
* builds a strict upper triangular matrix by filtering all tuples where id1 < id2
* output as csv

### Calculate Similarity
* Name: calculateSimilarity
* arg1: path/to/NameOfCrossedAndMerged.csv
* arg2: path/to/output/directory

Description:
* imports the NameOfCrossedAndMerged.csv
* runs 4 similarity algorithms: naive a, naive b, sort merge and simmetrics
* output 4 csv files in the output directory

## Data Structure
*concept.csv* columns: entity id, uri, source  
*concept_attributes.csv* columns: entity id, property name, property value, property type  
*linksWithIDs.csv* columns: source entity id, target entity id  
