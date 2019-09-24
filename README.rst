============
hopsworks-cloud-sdk
============

|Downloads| |PypiStatus| |PythonVersions|

`hopsworks-cloud-sdk` is an SDK to integrate existing cloud solutions such as Amazon SageMaker our Databricks with the Hopsworks platform.

It enables accessing the Hopsworks feature store from SageMaker and Databricks notebooks.

-----------
Quick Start
-----------

To Install:

>>> pip install hopsworks-cloud-sdk

Sample usage:

>>> from hops import featurestore
>>> featurestore.connect('ec2-w-x-y-z.us-east-2.compute.amazonaws.com', 'my_hopsworks_project')
>>> features_df = featurestore.get_features(["my_feature_1", "my_feature_2"])

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
  features_df = featurestore.get_features(
      ["team_budget", "average_attendance", "average_player_age",
      "team_position", "sum_attendance",
       "average_player_rating", "average_player_worth", "sum_player_age",
       "sum_player_rating", "sum_player_worth", "sum_position",
       "average_position"
      ]
  )
  featurestore.create_training_dataset(features_df, "team_position_prediction", data_format="tfrecords")

  def create_tf_dataset():
      dataset_dir = featurestore.get_training_dataset_path("team_position_prediction")
      input_files = tf.gfile.Glob(dataset_dir + "/part-r-*")
      dataset = tf.data.TFRecordDataset(input_files)
      tf_record_schema = featurestore.get_training_dataset_tf_record_schema("team_position_prediction")
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

**Integration with PyTorch**:

.. code-block:: python

  from hops import featurestore
  df_train=...
  featurestore.create_training_dataset(df_train, "MNIST_train_petastorm", data_format="petastorm")

  from petastorm.pytorch import DataLoader
  train_dataset_path = featurestore.get_training_dataset_path("MNIST_train_petastorm")
  device = torch.device('cuda' if use_cuda else 'cpu')
  with DataLoader(make_reader(train_dataset_path, num_epochs=5, hdfs_driver='libhdfs', batch_size=64) as train_loader:
          model.train()
          for batch_idx, row in enumerate(train_loader):
              data, target = row['image'].to(device), row['digit'].to(device)

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

References
--------------

- https://github.com/jupyter-incubator/sparkmagic/blob/master/examples/Magics%20in%20IPython%20Kernel.ipynb

.. |Downloads| image:: https://pepy.tech/badge/hops
   :target: https://pepy.tech/project/hopsworks-cloud-sdk
.. |PypiStatus| image:: https://img.shields.io/pypi/v/hops.svg
    :target: https://pypi.org/project/hopsworks-cloud-sdk
.. |PythonVersions| image:: https://img.shields.io/pypi/pyversions/hops.svg
    :target: https://travis-ci.org/hopsworks-cloud-sdk

------------------------
Development Instructions
------------------------

For development details such as how to test and build docs, see this reference: Development_.

.. _Development: ./Development.rst
