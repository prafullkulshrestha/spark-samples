package spring.spark.demo;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;




@SpringBootApplication
public class SpringSparkApplication {

	private static final Logger log = Logger.getLogger(SpringSparkApplication.class);

	@Value("${spring.profiles.active:default}")
	private String profiles;

	@Value("${flight.id:0}")
	private int id;

	@Autowired
	private AnalyzeService aService;

	// Use the run method when the app is launched as a job on the cluster
	public void run(String... args) {
		System.out.println("Helloooooo1234");
		if (profiles.indexOf("web") < 0) {
			log.warn("Web profile not declaired, running as a command line application.\nParameters:");
			log.warn("\tflight.id (default 0)");
			log.warn("\tfile.directory (default 0)");
			log.warn("\nAnalysis of flight ["+id+"]:\n" );
			aService.analyzeFlight(id);
			System.exit(0);
		}
	}

	
	public static void main(String[] args) {
		System.out.println("Helloooooo");
		ApplicationContext context = SpringApplication.run(SpringSparkApplication.class, args);
		SpringSparkApplication springSparkApplication = context.getBean(SpringSparkApplication.class);
		springSparkApplication.run();
	}
}
