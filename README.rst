===================
hopsworks-cloud-sdk
===================

|Downloads| |PypiStatus| |PythonVersions|

.. |Downloads| image:: https://pepy.tech/badge/hopsworks-cloud-sdk
   :target: https://pepy.tech/project/hopsworks-cloud-sdk
.. |PypiStatus| image:: https://img.shields.io/pypi/v/hopsworks-cloud-sdk.svg
    :target: https://pypi.org/project/hopsworks-cloud-sdk
.. |PythonVersions| image:: https://img.shields.io/pypi/pyversions/hopsworks-cloud-sdk.svg
    :target: https://travis-ci.org/hopsworks-cloud-sdk

`hopsworks-cloud-sdk` is an SDK to integrate existing cloud solutions such as Amazon SageMaker our Databricks with the Hopsworks platform.

It enables accessing the Hopsworks feature store from SageMaker and Databricks notebooks.

-----------
Quick Start
-----------

Ensure that your Hopsworks installation is set up correctly: `Setting up Hopsworks for the cloud <https://hops.readthedocs.io/en/latest/user_guide/hopsworks/featurestore.html#connecting-from-amazon-sagemaker>`_

To Install:

>>> pip install hopsworks-cloud-sdk

Sample usage:

>>> from hops import featurestore
>>> featurestore.connect('ec2-w-x-y-z.us-east-2.compute.amazonaws.com', 'my_hopsworks_project')
>>> features_df = featurestore.get_features(["my_feature_1", "my_feature_2"])

--------
Examples
--------

`Examples for using the Cloud SDK on SageMaker <https://nbviewer.jupyter.org/github/logicalclocks/hops-examples/blob/master/notebooks/featurestore/aws/SageMakerFeaturestoreTourPython.ipynb>`_

------------------------------------
Documentation
------------------------------------

API for the Hopsworks Feature Store
--------------------------------------------------------------------
Hopsworks has a data management layer for machine learning, called a feature store.
The feature store enables simple and efficient versioning, sharing, governance and definition of features that can be used to both train machine learning models or to serve inference requests.
The featurestore serves as a natural interface between data engineering and data science.

**Reading from the featurestore**:

.. code-block:: python

  from hops import featurestore
  features_df = featurestore.get_features(["team_budget", "average_attendance", "average_player_age"])

**Integration with Sci-kit Learn**:

.. code-block:: python

  from hops import featurestore
  train_df = featurestore.get_featuregroup("iris_features", dataframe_type="pandas")
  x_df = train_df[['sepal_length', 'sepal_width', 'petal_length', 'petal_width']]
  y_df = train_df[["label"]]
  X = x_df.values
  y = y_df.values.ravel()
  iris_knn = KNeighborsClassifier()
  iris_knn.fit(X, y)

**Integration with Tensorflow**:

.. code-block:: python

  from hops import featurestore
  feature_list = ["team_budget", "average_attendance", "average_player_age",
      "team_position", "sum_attendance",
      "average_player_rating", "average_player_worth", "sum_player_age",
      "sum_player_rating", "sum_player_worth", "sum_position",
      "average_position"
    ]

  latest_version = featurestore.get_latest_training_dataset_version("team_position_prediction")
  featurestore.create_training_dataset(
      features = feature_list,
      training_dataset = "team_position_prediction",
      descriptive_statistics = False,
      feature_correlation = False,
      feature_histograms = False,
      cluster_analysis = False,
      training_dataset_version = latest_version + 1
  )

  def create_tf_dataset():
      dataset_dir = featurestore.get_training_dataset_path("team_position_prediction")
      input_files = tf.gfile.Glob(dataset_dir + "/part-r-*")
      dataset = tf.data.TFRecordDataset(input_files)
      tf_record_schema = ... # Add tf schema
      feature_names = ["team_budget", "average_attendance", "average_player_age", "sum_attendance",
           "average_player_rating", "average_player_worth", "sum_player_age", "sum_player_rating", "sum_player_worth",
           "sum_position", "average_position"
          ]
      label_name = "team_position"

      def decode(example_proto):
          example = tf.parse_single_example(example_proto, tf_record_schema)
          x = []
          for feature_name in feature_names:
              x.append(example[feature_name])
          y = [tf.cast(example[label_name], tf.float32)]
          return x,y

      dataset = dataset.map(decode).shuffle(SHUFFLE_BUFFER_SIZE).batch(BATCH_SIZE).repeat(NUM_EPOCHS)
      return dataset

  tf_dataset = create_tf_dataset()

**Feature Visualizations**:

.. _feature_plots1.png: imgs/feature_plots1.png
.. figure:: imgs/feature_plots1.png
    :alt: Visualizing feature distributions
    :target: `feature_plots1.png`_
    :align: center
    :scale: 75 %
    :figclass: align-center


.. _feature_plots2.png: imgs/feature_plots2.png
.. figure:: imgs/feature_plots2.png
    :alt: Visualizing feature correlations
    :target: `feature_plots2.png`_
    :align: center
    :scale: 75 %
    :figclass: align-center

------------------------
Development Instructions
------------------------

For development details such as how to test and build docs, see this reference: Development_.

.. _Development: ./Development.rst
