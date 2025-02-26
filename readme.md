## How to Run

### 1. Clone the Repository:

```bash
git clone https://github.com/Alexiejow/geonames_predictx.git
cd geonames_predictx
```

### 2. Set Up the Virtual Environment:

```bash
python3 -m venv venv
source venv/bin/activate    # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Run the Pipeline using Spark:

To download the dataset, filter locations, assign metropolies, and generate visualizations, run:

```bash
python scripts/run_pipeline_spark.py
python scripts/visualise_spark.py
```

As a default, these are set to download and visualise data for "PL".
Within **run_pipeline_spark.py**, can the **dataset** variable to other [country codes](http://download.geonames.org/export/dump), or even *allCountries*.

The *visualise_spark.py* file can only create a visualisation for one country. In the *main()* function, apart from the country code you can specify a *draw_lines* variable - if it should include the lines between non-metroes and metroes, as well as set *filter_assigned* - if it should only include the assigned non-metroes.

Additionally, you can run the script below to create one, merged CSV for a selected country.
```bash
python scripts/merge_csvs.py
```

---

## Task and the solution

### Objective
Process the Geonames dataset ([PL.zip](http://download.geonames.org/export/dump/PL.zip)) to identify metropolitan cities and assign nearby towns to them.

The data is not saved locally, but downloaded straight from the url and processed in-memory.

### Steps
1. **Filter Residential Locations**: Extract cities, towns, and villages while excluding non-residential places. ([Feature Codes](http://www.geonames.org/export/codes.html))

    - Based on an exploratory data analysis of the Polish data, locations with the following codes are treated as populated, even if their population is 0 (proved to be incorrect on further checks): 'PPL', 'PPLA', 'PPLA2', 'PPLA3', 'PPLA4', 'PPLA5', 'PPLC'
    - An additional step was attempted for deduplicating locations and populations (e.g. Warsaw and its districts can all be treated as separate cities). This was done through:

        a) fetching borders polygons for selected cities via Geoapify API

        b) only maintaining the "main" location (like the one with administrative status PPLA or largest population) while

        c) deleting other locations that were nested within these borders

        The step was excluded because of the API limits and drastic increase of execution time, but a visualisation of its effects can be found in this [GDrive folder](https://drive.google.com/drive/folders/1WdAGNW6h81sEqH6zA9Sei0ffBdJEYQSj?usp=sharing)

2. **Identify Metropolitan Cities**

    A city is classified as metropolitan (metro) if:

    - It has a population of at least 10% of the largest city's population.
Example: If the largest city has 1,000,000 people, the metro threshold is 100,000.
    - If it is classified as PPL, it must also:
Not be in the same admin2_code as any PPLA or PPLC city.
This prevents large districts within major cities from being separately classified as metros.
This ensures that major urban centers are correctly identified, while avoiding inflating the number of metros by counting administrative districts separately.
3. **Assign Cities to Metropolises**

    Once metros are identified, all non-metro towns are assigned to the nearest metro based on:

    3.1. **Distance & Influence Radius**
    Each metro is given an influence radius, calculated based on its population. 
    
    3.2. **Efficient Spatial Matching with Bounding Boxes**
    
    Instead of a full cross-join (matching every non-metro with every metro), we use bounding boxes:

    a) Each metro defines a bounding box with latitude/longitude min/max values based on its influence radius.

    b) Only non-metros within a metro’s bounding box are considered as potential candidates.

    3.3. **Assigning the Best Metro for Each Town**
    
    Within a metro’s influence radius, haversine distance (great-circle distance) is computed.
    
    A force-based model is used:
The closer a town is to a metro, the stronger its influence.
Each town is assigned to the metro with the strongest influence.

### Spark implementation
Apache Spark is used to handle the large-scale processing of the GeoNames dataset efficiently. When processing *allCountries*, the dataset is repartitioned by country, so each parallel process can access all necessary data for the task.

### Advanced Task - in progress
Iteratively expand metropolitan areas as they accumulate population from assigned cities, dynamically increasing their influence.

### Output & Visualization
The final dataset includes all cities, with each assigned to a metro where applicable.

Data is saved in partitioned CSV files per country. Example below:


A map visualization can be generated to display:
    - Metros and their assigned towns.
    - The influence radius and connections.
    - Interactive tooltips with city information.

## Exploratory Data Analysis Summary

The EDA (see /notebooks folder) includes:

- **Data Loading:** Importing raw GeoNames data and setting appropriate headers.
- **Population Filtering:** Distinguishing between locations with zero and non-zero populations, and analyzing various feature classes (e.g., PPL, PPLA, PPLC).
- **Location Categories:** Identifying and analyzing different categories of populated places and administrative units, including duplicate names and overlaps.
- **Visualization:** Initial maps displaying locations in Poland, with markers color-coded based on feature codes.

### Filtering of nested locations
During EDA, it was observed that many smaller locations (e.g., districts or subdivisions) are nested within the borders of larger administrative areas. To improve clarity, a feature was implemented to remove locations that are already included in the borders of larger cities.

This step helps reduce visual clutter and ensures the final visualization displays only distinct, non-overlapping locations.

---

## Future Enhancements

- **Iterative Metropolis Assignment:** Implement an iterative algorithm where metropolises "grow" by annexing nearby towns, updating effective populations, and reassigning until convergence.
- **Map Visualizations per Iteration:** Generate a series of interactive maps to visualize each iteration of the assignment process.


---

## License

This project is licensed under the MIT License.

