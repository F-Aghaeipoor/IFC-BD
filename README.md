# IFC-BD

This is the core library for the paper published in IEEE Transactions on Fuzzy Systems as "IFC-BD: An Interpretable Fuzzy Classifier for Boosting Explainable Artificial Intelligence in Big Data
".

**Running Command :**

After preparing  "start.jar" it can be run as  :  /opt/spark-2.2.0/bin/spark-submit --class Core.Start --master spark://<ServerName>:<portNum> --executor-memory 6G --total-executor-cores start.jar $1 $2 $3 $4 $5 $6 $7 $8 $9 ${10} ${11} ${12}

The parameters are (See Core.Parameters): 
susy susy ${fold} data 3 winning-rule true true -1  false false 0.7


**Please cite as: ** F. Aghaeipoor, M. M. Javidi and A. Fernandez, "IFC-BD: An Interpretable Fuzzy Classifier for Boosting Explainable Artificial Intelligence in Big Data," in IEEE Transactions on Fuzzy Systems, doi: 10.1109/TFUZZ.2021.3049911.
See [here!](https://ieeexplore.ieee.org/document/9316882)

**BibTeX format:**

@ARTICLE{Aghaeipoor2021,

  author={F. {Aghaeipoor} and M. M. {Javidi} and A. {Fernandez}},
  
  journal={IEEE Transactions on Fuzzy Systems}, 
  
  title={IFC-BD: An Interpretable Fuzzy Classifier for Boosting Explainable Artificial Intelligence in Big Data}, 
  
  year={2021},
  
  volume={},
  
  number={},
  
  pages={1-1},
  
  doi={10.1109/TFUZZ.2021.3049911}}
