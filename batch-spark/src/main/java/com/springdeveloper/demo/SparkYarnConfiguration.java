package com.springdeveloper.demo;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.hadoop.batch.scripting.ScriptTasklet;
import org.springframework.data.hadoop.scripting.HdfsScriptRunner;
import org.springframework.scripting.ScriptSource;
import org.springframework.scripting.support.ResourceScriptSource;

@Configuration
public class SparkYarnConfiguration {

	@Autowired
	private org.apache.hadoop.conf.Configuration hadoopConfiguration;

	@Value("${demo.inputDir}")
	String inputDir;
	
	@Value("${demo.inputFileName}")
	String inputFileName;
	
	@Value("${demo.inputLocalDir}")
	String inputLocalDir;
	
	@Value("${demo.outputDir}")
	String outputDir;
	
	@Value("${demo.sparkAssembly}")
	String sparkAssembly;
	
	// Job definition
	@Bean
	Job tweetHashtags(JobBuilderFactory jobs, Step initScript, Step sparkTopHashtags) throws Exception {
	    return jobs.get("TweetTopHashtags")
	    		.start(initScript)
	    		.next(sparkTopHashtags)
	    		.build();
	}
	 
	// Step 1 - Init Script
	@Bean
    Step initScript(StepBuilderFactory steps, Tasklet scriptTasklet) throws Exception {
		return steps.get("initScript")
    		.tasklet(scriptTasklet)
            .build();
    }

	@Bean
	ScriptTasklet scriptTasklet(HdfsScriptRunner scriptRunner) {
		ScriptTasklet scriptTasklet = new ScriptTasklet();
		scriptTasklet.setScriptCallback(scriptRunner);
		return scriptTasklet;
	}

	@Bean HdfsScriptRunner scriptRunner() {
		ScriptSource script = new ResourceScriptSource(new ClassPathResource("fileCopy.js"));
		HdfsScriptRunner scriptRunner = new HdfsScriptRunner();
		scriptRunner.setConfiguration(hadoopConfiguration);
		scriptRunner.setLanguage("javascript");
		Map<String, Object> arguments = new HashMap<>();
		arguments.put("source", inputLocalDir);
		arguments.put("file", inputFileName);
		arguments.put("indir", inputDir);
		arguments.put("outdir", outputDir);
		scriptRunner.setArguments(arguments);
		scriptRunner.setScriptSource(script);
		return scriptRunner;
	}

	// Step 2 - Spark Top Hashtags
	@Bean
    Step sparkTopHashtags(StepBuilderFactory steps, Tasklet sparkTopHashtagsTasklet) throws Exception {
		return steps.get("sparkTopHashtags")
    		.tasklet(sparkTopHashtagsTasklet)
            .build();
    }

	@Bean
	MySparkYarnTasklet sparkTopHashtagsTasklet() throws Exception {
		MySparkYarnTasklet sparkTasklet = new MySparkYarnTasklet();
		sparkTasklet.setSparkAssemblyJar(sparkAssembly);
		sparkTasklet.setHadoopConfiguration(hadoopConfiguration);
		sparkTasklet.setAppClass("Hashtags");
		System.out.println("************************** " + System.getProperty("user.dir"));
		File jarFile = new File(System.getProperty("user.dir") + "/app/spark-hashtags_2.10-0.1.0.jar");
		sparkTasklet.setAppJar(jarFile.toURI().toString());
		sparkTasklet.setExecutorMemory("1G");
		sparkTasklet.setNumExecutors(1);
		sparkTasklet.setArguments(new String[]{"0000",
				hadoopConfiguration.get("fs.default.name") + inputDir + "/" + inputFileName, 
				hadoopConfiguration.get("fs.default.name") + outputDir});
		return sparkTasklet;
	}
}
