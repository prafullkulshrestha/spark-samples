package spring.spark.demo.rdd;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import scala.Tuple2;
import spring.spark.demo.AnalyzeService;


@Service
public class BatchAnalyzer implements AnalyzeService {
	private static final Logger log = Logger.getLogger(BatchAnalyzer.class);

	@Value("${file.directory}")
	private String directory;

	private final JavaSparkContext sc;

	@Autowired
	public BatchAnalyzer(JavaSparkContext sc) {
		this.sc = sc;
	}

	@Override
	public void analyzeFlight(int id) {
		log.info("Analyzing flight: " + id);

		Map<String, Integer> retVal = null;

		// Load the lines and cache them to do multiple processing runs
		JavaRDD<String> lines = sc.textFile(directory + "20130316-DATA-00.TXT")
				.cache();
		System.out.println(Arrays.toString(lines.take(5).toArray()));

	}

	
}
