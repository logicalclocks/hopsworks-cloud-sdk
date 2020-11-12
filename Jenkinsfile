pipeline {
  agent {
    node {
      label 'local'
    }
  }

  stages {
    stage ('setup') {
      steps {
        // Creates the virtualenv before proceeding
        sh """
        if [ ! -d $WORKSPACE/../hopsworks-cloud-sdk-env ];
        then
          virtualenv --python=/usr/bin/python3 $WORKSPACE/../hopsworks-cloud-sdk-env
        fi
	$WORKSPACE/../hopsworks-cloud-sdk-env/bin/pip install twine sphinx sphinx-autobuild recommonmark sphinx_rtd_theme jupyter_sphinx_theme readme_renderer[md]
        rm -rf dist/*
        """
      }
		}
    stage ('build') {
      steps {
        sh """
        source $WORKSPACE/../hopsworks-cloud-sdk-env/bin/activate
        python ./setup.py sdist
        """
      }
    }
    stage ('deploy-bin') {
      environment {
        PYPI = credentials('977daeb0-e1c8-43a0-b35a-fc37bb9eee9b')
      }
      steps {
        sh """
        source $WORKSPACE/../hopsworks-cloud-sdk-env/bin/activate
    	  twine upload -u $PYPI_USR -p $PYPI_PSW --skip-existing dist/*
        """
      }
    }
    stage ('deploy-doc') {
      steps {
        sh """
        set -x
        scp -r docs/_build/html/* jenkins@hops-py.logicalclocks.com:/var/www/hopsworks-cloud-sdk;
        """
      }
    }
  }
}
