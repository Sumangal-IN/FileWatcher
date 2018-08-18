
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileWatcher {
	private static final String ALPHA_NUMERIC_STRING = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	private static String elastic_url = null;

	public static void main(String[] args) throws InterruptedException {
		if (args.length < 3) {
			System.out.println("Provide path, file to watch and elastic URL!");
			return;
		}
		String path = args[0];
		String filename = args[1];
		elastic_url = args[2];
		ArrayList<String> new_lines = new ArrayList<>();
		try {
			WatchService watcher = FileSystems.getDefault().newWatchService();
			Path dir = Paths.get(path);
			dir.register(watcher, ENTRY_MODIFY);
			System.out.println("Watch Service registered for dir: " + path + "//" + filename);
			while (true) {
				WatchKey key;
				try {
					key = watcher.take();

					for (WatchEvent<?> event : key.pollEvents()) {
						WatchEvent.Kind<?> kind = event.kind();
						@SuppressWarnings("unchecked")
						WatchEvent<Path> ev = (WatchEvent<Path>) event;
						Path fileName = ev.context();
						if (kind == ENTRY_MODIFY && fileName.toString().equals(filename)) {
							BufferedReader br = null;
							int last_line = 0;
							// get last read line number
							File registryFile = new File(path + "\\" + filename + ".registry");
							if (registryFile.exists()) {
								br = new BufferedReader(new FileReader(registryFile));
								last_line = Integer.parseInt(br.readLine());
								br.close();
							}
							// get the changed lines
							br = new BufferedReader(new FileReader(path + "\\" + filename));
							String line = null;
							int current_line = 0;
							while ((line = br.readLine()) != null) {
								current_line++;
								if (current_line > last_line) {
									if (line.startsWith("LOG_WATCH"))
										new_lines.add(line);
								}
							}
							br.close();
							// update last read line number
							FileWriter fw = new FileWriter(registryFile, false);
							fw.write(Integer.toString(current_line));
							fw.close();
						}
					}
					boolean valid = key.reset();
					if (!valid) {
						break;
					}
					System.out.println(new_lines.size() + " lines collected");
					process(new_lines);
					new_lines.clear();
					Thread.sleep(5000);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	private static void process(ArrayList<String> new_lines) {
		for (String line : new_lines) {
			String entity = line.substring(line.indexOf(" ") + 1, line.indexOf(" ", line.indexOf(" ") + 1));
			line = line.substring(line.indexOf(" ", line.indexOf(" ") + 1) + 1);
			List<String> parameters = Arrays.asList(line.replace("[", "").replace("]", "").split(", "));
			String JSON = "{";
			for (String parameter : parameters) {
				JSON += "\"" + parameter.split("=")[0] + "\":\"" + parameter.split("=")[1] + "\",";
			}
			if (JSON.endsWith(","))
				JSON = JSON.substring(0, JSON.length() - 1);
			JSON += "}";
			System.out.println(JSON);
			String random_id = randomAlphaNumeric(20);
			try {
				System.out.println(elastic_url + "/log_watch/" + entity + "/" + random_id);
				URL url = new URL(elastic_url + "/log_watch/" + entity + "/" + random_id);
				HttpURLConnection conn = (HttpURLConnection) url.openConnection();
				conn.setDoOutput(true);
				conn.setRequestMethod("POST");
				conn.setRequestProperty("Content-Type", "application/json");
				OutputStream os = conn.getOutputStream();
				os.write(JSON.getBytes());
				os.flush();
				if (conn.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
					throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
				}
				conn.disconnect();
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static String randomAlphaNumeric(int count) {
		StringBuilder builder = new StringBuilder();
		while (count-- != 0) {
			int character = (int) (Math.random() * ALPHA_NUMERIC_STRING.length());
			builder.append(ALPHA_NUMERIC_STRING.charAt(character));
		}
		return builder.toString();
	}

	// SAMPLE LOG LINE
	// LOG_WATCH CallLog [id=123, callerSid=456, direction=IN, transcript=ok yes, intent=welcome, callTime=123T749]
	
	// SAMPLE COMMAND LINE PARAM
	// java -jar LogWatcher.jar C:\Users\Sumangal\Desktop\logs foo.log http://ec2-52-91-20-77.compute-1.amazonaws.com:9200
}