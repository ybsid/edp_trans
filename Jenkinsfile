#!/usr/bin/env groovy
@Library("com.optum.jenkins.pipeline.library@master") _

String getRepoName() {
    return "$GIT_URL".tokenize('/')[3].split("\\.")[0]
}

String getRepoOwnerName() {
    return "$GIT_URL".tokenize('/')[2].split("\\.")[0]
}

import com.optum.jenkins.pipeline.library.scm.Git
import com.optum.jenkins.pipeline.library.sonar.Sonar

pipeline {
    agent {
        label 'docker-maven-slave'
    }

    environment {
        APP_NAME = "edp_trans"
        IMAGE = "edp_trans"
        TAG = "$master"
        BUILD = "${env.BUILD_NUMBER}"
        IMAGE_TAG = "${IMAGE}/${BRANCH_NAME}"
        IMAGE_TAG_BUILD = "${IMAGE_TAG}-${BUILD}"
        JFROG_CREDS = 'bds_oso_id'
        NAMESPACE = 'bds_oso_id'
        GIT_URL = "https://github.optum.com/EligibilityBigData/edp_trans_component.git"
        BRANCH_NAME = "dev"


    }

    stages {
        stage('Code Checkout') {
            steps {
		echo '-------- Code checkout --------'
		checkout([$class: 'GitSCM', branches: [[name: '*/dev']],
                  doGenerateSubmoduleConfigurations: false,
                  submoduleCfg: [],
                  userRemoteConfigs: [[credentialsId: 'bds_oso_id'
                                       , url: 'https://github.optum.com/EligibilityBigData/edp_trans_component.git']]])
 		}
	    }

	    stage('Build') {
                steps {
    		//sh 'chmod +x mvnw'
    		glMavenBuild mavenGoals:'clean install', uploadJacocoResults : false, uploadUnitTestResults:false, runJacocoCoverage:false
                    glMavenBuild pomFile: "pom.xml",
                            additionalProps: [
                                    "scalatest.span.scale.factor": 10.0,
                                    "scalatest.log.options": "W"
                            ],
                            runJacocoCoverage: false,
                            uploadJacocoResults: false
    	    }
    	 }

        /*
       stage("Fortify Scan") {
            agent { label 'docker-fortify-slave' }
            steps {
                script {
                    try {
                        last_run_stage = "Sonar and Fortify Scans"
                        glFortifyScan fortifyBuildName: APP_NAME,
                                criticalThreshold: 600,
                                highThreshold: 600,
                                mediumThreshold: 600,
                                lowThreshold: 600,
                                isGenerateDevWorkbook: true,
                                sourceDirectory: env.WORKSPACE,
                                failBuildWhenThresholdPassed: false,
                                archiveArtifacts: true,
                                downloadScan: true,
                                uploadScan: true
                    } catch (err) {
                        echo "Fortify failed: " + err
                    }
                }
            }
        }*/
        stage('Sonar Scan') {
            when {
                expression {
                    return pipelineParams.runSonar == true;
                }
            }
            steps {
                glSonarMavenScan productName: "edp_trans_component_coverage",
                        additionalProps: [
                                "sonar.sources" : "src/main" ,
                                "sonar.scala.coverage.reportPaths" : "target/scoverage.xml"
                        ],
                        isDebugMode: true,
                        isBatchMode: false,
                        gitUserCredentialsId:  pipelineParams.sonarCredentials
            }
        }
    }

post {
  always {
    echo 'Jenkins Job is completed with the below status'
  }
  success {
      echo 'Jenkins Job is completed successfully'
      emailext body: "Hello Team, \n\n Build URL: ${GIT_URL} \nBuild triggered and the status of the build is Success \n\n Thanks and Regards, \n Team Voyger",
      subject: "$currentBuild.currentResult-$JOB_NAME: Success ",
      to: 'kumar_siddharth@optum.com'
  }
  failure {
      echo 'FileProcess Jenkins Job failed'
      emailext body: "Hello Team , \n\n Build URL: ${GIT_URL} \nBuild triggered and the status of the build is Failed \n\n Thanks and Regards, \n Team Voyger",
      subject: "$currentBuild.currentResult-$JOB_NAME: Failed ",
      to: 'kumar_siddharth@optum.com'
  }
   unstable {
       echo 'This will run only if the run was marked as unstable'
   }
   changed {
       echo 'This will run only if the state of the Pipeline has changed'
       echo 'For example, if the Pipeline was previously failing but is now successful'
   }
   aborted {
       echo 'No approval'
   }
}
}