# bdp-string-sim
The String Similarity on Flink Project from the Big Data Praktikum @ UNI Leipzig, SS2016

## Requirements
'/src/main/resources/linklion' and '/src/main/resources/perfect' is excluded from repo.

Please be sure that all necessary *.csv files are in the following directory structure:
|-src
  |-main
    |-java
    |-resources
      |-linklion
        |-concept.csv
        |-concept_attributes.csv
        |-linksWithIDs.csv
      |-perfect
        |-concept.csv
        |-concept_attributes.csv
        |-linksWithIDs.csv

## Data Structure
concept.csv columns: entity id, uri, source
concept_attributes.csv columns: entity id, property name, property value, property type
linksWithIDs.csv columns: source entity id, target entity id