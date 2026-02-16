package utils.tools;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import play.Logger;

/**
 * Reads .twee files and converts them to javascript codes Configuration options are included: filename, custom sorry
 * message, dataset ids, scheduling options For scheduling two files should be generated, one based on the .twee file
 * and one standard for scheduling
 * 
 * Files that should be kept: fixed_basic.js fixed_scheduling_telegram.js templateSchedulingScript.js
 * 
 * @author Anniek Jansen
 */
public class ReadTweeFile {
	public static int countList;
	public static boolean openQuestion;
	public static String tweeFileName;
	public static boolean scheduledConfig;
	public static boolean firstMessageScheduled;
	public static String endMessage = "You have reached the end for now, thank you!"; // this is the default end
	                                                                                  // message,
	                                                                                  // users can change it during the
	                                                                                  // configuration
	public static String sorryMessage = "Sorry there is no response for this input"; // this is the default message when
	                                                                                 // the message is not recognized,
	                                                                                 // users can change it during the
	                                                                                 // configuration
	public static int datasetID;
	public static int entityID;
	public static String firstMessage;
	public static List<String> keys = new ArrayList<>();
	public static List<String> linkNames = new ArrayList<>();
	public static List<String> goTos = new ArrayList<>();
	public static List<String> variableNames = new ArrayList<>();
	public static List<String> keysVar = new ArrayList<>();
	public static List<String> variableValues = new ArrayList<>();

	private static final Logger.ALogger logger = Logger.of(ReadTweeFile.class);

	public static void main(String[] args) {
		config();
		String keyRegEx = ":: (.*?) \\{.*\\}";
		String keyRegExScheduled = ":: (.*?) \\[.*\\] \\{.*\\}";
		String scheduleTimeRegEx = ":: .*?\\[(.*)\\] \\{.*\\}";
		String startRegEx = " \"start\": \"(\\w*)\"";//
		String buttonRegEx_relink = "\\[\\[.*?->.*?\\]\\]";
		String buttonRegEx_relink_g1 = "\\[\\[(.*?)->.*?\\]\\]";
		String buttonRegEx_relink_g2 = "\\[\\[.*?->(.*?)\\]\\]";
		String buttonRegEx = "\\[\\[(.*?)\\]\\]";
		String key;
		String scheduled;
		String fileName = "";
		String start = "";
		boolean setupFinished = false;
		boolean inPanel = false;

		// Read and convert the .twee file
		try {
			Scanner scanner = new Scanner(new File(tweeFileName));
			String currentLine = "";

			while (scanner.hasNextLine()) {
				// If the title and start point are not yet defined, read the next line and
				// search for matches
				while (!setupFinished) {
					currentLine = scanner.nextLine();
					if (checkMatch(currentLine, ":: StoryTitle")) {
						// Save the name of the storytitle to fileName and use this a document title for
						// the .js file
						fileName = scanner.nextLine();
						logger.info("title: " + fileName);
						writeToFile(fileName, "init", "");
					} else if (checkMatch(currentLine, ":: StoryData")) {
						// Scan over the next lines until the line with "start"
						String nextLine = scanner.nextLine();
						while (!checkMatch(nextLine, "start")) {
							nextLine = scanner.nextLine();

						}
						// Save the starting point to the start variable
						// Finish the setup phase (setupFinished=true)
						if ((checkMatch(nextLine, "start"))) {
							start = returnGroup(nextLine, startRegEx).toLowerCase();
							logger.info("Start at: " + start);
							setupFinished = true;
							currentLine = scanner.nextLine();
						}
					}
				}

				// Continue scanning next lines it encounters a :: which indicates the
				// start of a panel
				if (!inPanel) {
					currentLine = scanner.nextLine();
				}
				// Read and store the information from a panel in the .twee file
				// When :: StoryScript is found, it breaks out of the loop since this
				// information does not need to be converted to the .js file
				if (checkMatch(currentLine, ":: StoryScript")) {
					break;
				} else if (checkMatch(currentLine, "::")) {
					// set inPanel true to prevent skipping lines (otherwise it will skip one panel
					// each time)
					inPanel = true;
					countList = 1; // this is used for numbered lists
					openQuestion = false; // false by default
					// save the text after :: as the key (all keys are lowercase)
					// also extract the scheduling tag if scheduling is used, otherwise it remains
					// empty
					if (checkMatch(currentLine, keyRegExScheduled)) {
						key = returnGroup(currentLine, keyRegExScheduled).toLowerCase();
						scheduled = returnGroup(currentLine, scheduleTimeRegEx);
					} else {
						key = returnGroup(currentLine, keyRegEx).toLowerCase();
						scheduled = "";
					}

					String nextLine = scanner.nextLine();
					String txt = "";
					String next = "";
					String scheduledText = "";
					String scheduledNext = "";

					// As long as there is no :: indicating a new panel and there are still next
					// lines, loop over the text and append it to a String
					// This will be the text for the chatbot
					while ((!checkMatch(nextLine, "::")) && scanner.hasNextLine()) {

						txt = appendText(nextLine, txt);
						nextLine = scanner.nextLine();

					}
					// ensures that also the last line is appended
					if (!scanner.hasNextLine()) {
						txt = appendText(nextLine, txt);
					}

					// if "---" is found in the file, split the text on this section. The text above
					// the --- will be send at the scheduled time, the text below will be send
					// directly
					if (checkMatch(txt, "---")) {
						String[] splitTxt = txt.split("---");
						// logger.info(splitTxt[0]);
						// logger.info(splitTxt[1]);
						txt = splitTxt[1];
						scheduledText = splitTxt[0];
						if (checkMatch(txt, buttonRegEx_relink)) {
							next = returnNext(txt, buttonRegEx_relink_g2).toLowerCase();
							txt = appendAndReturnLinkName(txt, buttonRegEx_relink_g1, buttonRegEx_relink_g2, key);
							// txt = txt.replaceAll(buttonRegEx_relink, "[[" + linkName + "]]");
						} else if (checkMatch(scheduledText, buttonRegEx_relink)) {
							scheduledNext = returnNext(txt, buttonRegEx_relink_g2).toLowerCase();
							scheduledText = appendAndReturnLinkName(txt, buttonRegEx_relink_g1, buttonRegEx_relink_g2,
							        key);
							// scheduledText = scheduledText.replaceAll(buttonRegEx_relink, "[[" + linkName
							// + "]]");

						} else {
							next = returnNext(txt, buttonRegEx).toLowerCase();
							scheduledNext = returnGroup(scheduledText, buttonRegEx).toLowerCase();
						}

					} else {
						if (checkMatch(txt, buttonRegEx_relink)) {
							next = returnNext(txt, buttonRegEx_relink_g2).toLowerCase();
							txt = appendAndReturnLinkName(txt, buttonRegEx_relink_g1, buttonRegEx_relink_g2, key);

						} else {
							next = returnNext(txt, buttonRegEx).toLowerCase();
						}
					}

					// remove the button in case of an open question
					// not done before as it is needed to determine the next panel
					if (openQuestion) {
						txt = txt.replaceAll(buttonRegEx, "");
						scheduledText = scheduledText.replaceAll(buttonRegEx, "");
					}

					// include scheduled if config set to true:
					String result = "";
					if (scheduledConfig) {
						if (!scheduled.equals("") && scheduledText.equals("")) {
							scheduledText = txt; // text above ---
							scheduledNext = next; // next above ---
							txt = ""; // text below ---
							next = ""; // next below ---
						}
						// For a scheduled first message, the next button will be removed to prevent the
						// user from moving on
						if (key.equals(start)) {
							txt = txt.replaceAll(buttonRegEx, "");
						}

						// replace quotes with backslash quote
						txt = txt.replace("\"", "\\\"");
						scheduledText = scheduledText.replace("\"", "\\\"");

						// replace + define variables
						txt = replaceVariables(txt);
						txt = defineVariable(txt, key);
						scheduledText = replaceVariables(scheduledText);
						scheduledText = defineVariable(scheduledText, key);

						// the json line for a scheduled message
						result = "chatSM[\"" + key + "\"]={text: \"" + txt + "\", next: \"" + next
						        + "\", openQuestion: \"" + openQuestion + "\", scheduledAt: \"" + scheduled
						        + "\", scheduledText: \"" + scheduledText + "\", scheduledNext: \"" + scheduledNext
						        + "\"}";
					} else {
						txt = txt.replace("\"", "\\\""); // replace quotes with backslash quote

						// replace + define variables
						txt = replaceVariables(txt);
						txt = defineVariable(txt, key);

						// the json line for a normal message
						result = "chatSM[\"" + key + "\"]={text: \"" + txt + "\", next: \"" + next
						        + "\", openQuestion: \"" + openQuestion + "\"}";

					}
					// Write this line of code to the .js file
					writeToFile(fileName, result, "");

					currentLine = nextLine;
				}

			}

			// Once all lines are read, write the remaining part of the code
			// Include the start panel as this needs to be defined
			// txt=end is used in function to determine what action to take
			writeToFile(fileName, "end", start);

			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		// Create the code for the scheduling script
		// This could be more efficient by just returning the the
		// templateSchedulingScript.js instead of copying it to a new file because no
		// changes are made
		if (scheduledConfig) {
			createSchedulingScript("schedulingScript", "templateSchedulingScript.js");
		}
	}

	// Configuration options
	static void config() {
		// String tempEndMessage;
		String tempSorryMessage;
		Scanner scannerTweeFileName = new Scanner(System.in);
		logger.info("Enter file name of the .twee file (<filename>.twee), case sensitive");
		tweeFileName = scannerTweeFileName.nextLine();
		logger.info("Configuration options");
		// logger.info(
		// "Please enter the message that should be send in the end to the participant
		// (or leave empty to use default): ");
		// tempEndMessage = scannerTweeFileName.nextLine();
		logger.info(
		        "Please enter the message that should be send in case the participants enters an incorrect input (or leave empty to use default): ");
		tempSorryMessage = scannerTweeFileName.nextLine();
		logger.info("Please enter the dataset ID of your IoT dataset in which you want to save the responses:");
		datasetID = scannerTweeFileName.nextInt();
		logger.info("Please enter the dataset ID of your entity dataset:");
		entityID = scannerTweeFileName.nextInt();
		logger.info("Do you want to use scheduling in your chatbot? (true/false)");
		scheduledConfig = scannerTweeFileName.nextBoolean();

		if (scheduledConfig) {
			logger.info(
			        "Is the first message a scheduled group message (i.e. should it be send to all participants at the same time)? (true/false)");
			firstMessageScheduled = scannerTweeFileName.nextBoolean();
			if (firstMessageScheduled) {
				logger.info(
				        "The start point of your .twee file will be used as direct response when the user starts the chatbot, please enter here the key of the first message that is scheduled and should be send to all participants.");
				firstMessage = scannerTweeFileName.nextLine().toLowerCase();
				if (firstMessage.equals("")) {
					logger.info("The key (name of the passage in twine) is:");
					firstMessage = scannerTweeFileName.nextLine().toLowerCase();
				}
			}
		}

		// Check if the entered file name includes the .twee extension, if not append it
		if (tweeFileName.contains((".twee"))) {

		} else {
			tweeFileName = tweeFileName + ".twee";
		}

		// if the end and sorry message are not empty, replace the default messages
		// if (!tempEndMessage.equals("")) {
		// endMessage = tempEndMessage;
		// }

		if (!tempSorryMessage.equals("")) {
			sorryMessage = tempSorryMessage;
		}

		logger.info("tweeFileName is: " + tweeFileName);
		logger.info("Scheduled active: " + scheduledConfig);
		logger.info("First message scheduled: " + firstMessageScheduled);
		logger.info("First message key: " + firstMessage);
		// logger.info("End message: " + endMessage);
		logger.info("Sorry message: " + sorryMessage);
		scannerTweeFileName.close();
	}

	// Creates the scheduling script by just copying it from the template script, if
	// possible the template script can also just be returned
	static void createSchedulingScript(String fileNameScheduling, String fixedFileName) {
		try {
			FileWriter myWriter = new FileWriter(fileNameScheduling + ".js", false);
			File myFixedFile = new File(fixedFileName);
			Scanner myReader = new Scanner(myFixedFile);
			while (myReader.hasNextLine()) {
				String data = myReader.nextLine();
				myWriter.write("\n" + data);
				// logger.info(data);
			}
			myReader.close();
			myWriter.close();
		} catch (IOException e) {
			logger.info("An error occurred.");
			e.printStackTrace();
		}
	}

	static void writeToFile(String fileName, String txt, String start) {
		// Use txt to determine if the file need to be started, appended or finalized

		// Setting up the file, overwriting it if it already exists (append=false)
		// Adding meta data and the first lines of code that are needed
		if (txt == "init") {
			try {
				FileWriter myWriter = new FileWriter(fileName + ".js", false);
				myWriter.write("// File name: " + fileName + "\n");
				Instant instant = Instant.now();
				myWriter.write("// Timestamp of creation: " + instant + "\n");
				myWriter.write("// Subscribe to telegram \n");
				myWriter.write("\nvar profile = DF.entity.get(data.participant_id)\n");
				myWriter.write("\nvar key = profile.key\n");
				myWriter.write("var chatSM = {};");
				myWriter.close();
			} catch (IOException e) {
				logger.info("An error occurred.");
				e.printStackTrace();
			}
		} else if (txt == "end") {
			// In case of the end of the file, append to the file and several lines of code
			// First part is hardcoded as the start panel is different each time
			// Rest is read from fixed_basic.js/fixed_scheduling_telegram.js file and line
			// by line appended
			try {
				FileWriter myWriter = new FileWriter(fileName + ".js", true);
				myWriter.write("\n\n\n");
				StringBuilder jsCode = new StringBuilder("var dict = [\n");
				for (int i = 0; i < keys.size(); i++) {
					jsCode.append("  {\n");
					jsCode.append("    key: \"" + keys.get(i) + "\",\n");
					jsCode.append("    linkName: \"" + linkNames.get(i).toLowerCase() + "\",\n");
					jsCode.append("    goTo: \"" + goTos.get(i) + "\"\n");
					jsCode.append("  }");
					if (i < keys.size() - 1) {
						jsCode.append(",");
					}

					jsCode.append("\n");
				}
				jsCode.append("];");
				myWriter.write(jsCode.toString());

				StringBuilder varList = new StringBuilder("\nvar vars = [\n");
				for (int i = 0; i < keysVar.size(); i++) {
					varList.append(" {\n");
					varList.append(" key: \"" + keysVar.get(i) + "\",\n");
					varList.append(" var: \"" + variableNames.get(i).toLowerCase() + "\",\n");
					varList.append(" value: \"" + variableValues.get(i) + "\"\n");
					varList.append(" }");
					if (i < keys.size() - 1) {
						varList.append(",");
					}

					varList.append("\n");
				}
				varList.append("];");
				myWriter.write(varList.toString());

				myWriter.write("\nvar msg = data.message.toLowerCase()");
				// myWriter.write("\nvar endMessage = \"" + endMessage + "\"");
				myWriter.write("\nvar sorryMessage = \"" + sorryMessage + "\"");
				myWriter.write("\nvar datasetID = " + datasetID);
				myWriter.write("\nvar entityID = " + entityID);
				myWriter.write("\n// just starting out?");
				if (!firstMessageScheduled) {
					myWriter.write("\nif(profile[\"" + start + "\"] === undefined || profile[\"" + start
					        + "\"].length == 0) {");
					myWriter.write("\n\t  DF.telegramParticipant(data.participant_id, chatSM[\"" + start + "\"].text)");
					myWriter.write("\n\t  DF.entity.update(data.participant_id, {start: \"done\"})");
					myWriter.write("\n\t DF.entity.update(data.participant_id, { openQ: chatSM[\"" + start
					        + "\"].openQuestion })");
					myWriter.write("\n\t DF.entity.update(data.participant_id, { key: chatSM[\"" + start
					        + "\"].openQuestion })");
					myWriter.write("\n }");
				} else {
					myWriter.write("\nif(profile[\"" + start + "\"] === undefined || profile[\"" + start
					        + "\"].length == 0) {");
					myWriter.write("\n\t  DF.telegramParticipant(data.participant_id, chatSM[\"" + start + "\"].text)");
					myWriter.write("\n\t  DF.entity.update(data.participant_id, {start: \"pending\"})");
					myWriter.write("\n\tlet nextMessage = chatSM[\"" + firstMessage + "\"]");
					myWriter.write("\n\tlet studyStart = createNextTS(nextMessage.scheduledAt);");
					myWriter.write("\n\tlet scheduled = DF.entity.get(\"scheduled\");");
					myWriter.write("\n\n\tlet scheduledArray = [];");
					myWriter.write("\n\tif (!isObjEmpty(scheduled)) {\r\n" + //
					        "        for (let i = 0; i < scheduled.scheduledMessages.length; i++) {\r\n" + //
					        "            scheduledArray.push(scheduled.scheduledMessages[i]);\r\n" + //
					        "        }\r\n" + //
					        "    }");
					myWriter.write("\n    let newData = {\r\n" + //
					        "       \t pid: \"*\",\r\n" + //
					        "        scheduledAt: studyStart,\r\n" + //
					        "        message: nextMessage.scheduledText,\r\n" + //
					        "        openQ: nextMessage.openQuestion,\r\n" + //
					        "        key: \"" + firstMessage + "\",\r\n" + //
					        "        next: nextMessage.scheduledNext\r\n" + //
					        "    };\r\n" + //
					        "    scheduledArray.push(newData);\r\n" + //
					        "    DF.entity.update(\"scheduled\", {\r\n" + //
					        "       \"scheduledMessages\": scheduledArray\r\n" + //
					        "    })");
					myWriter.write("\n }");
				}
				try {
					String fixed_filename = "";

					if (scheduledConfig) {
						fixed_filename = "fixed_scheduling_telegram.js";

					} else {
						fixed_filename = "fixed_basic.js";
					}
					File myFixedFile = new File(fixed_filename);
					Scanner myReader2 = new Scanner(myFixedFile);
					while (myReader2.hasNextLine()) {
						String data = myReader2.nextLine();
						myWriter.write("\n" + data);
						// logger.info(data);
					}
					myReader2.close();
				} catch (FileNotFoundException e) {
					logger.info("An error occurred.");
					e.printStackTrace();
				}

				myWriter.close();
			} catch (IOException e) {
				logger.info("An error occurred.");
				e.printStackTrace();
			}
		} else {
			// If not init or end, append the file with the txt received
			try {
				FileWriter myWriter = new FileWriter(fileName + ".js", true);
				myWriter.write("\n" + txt);
				myWriter.close();
				// logger.info("Successfully wrote to the file.");
			} catch (IOException e) {
				logger.info("An error occurred.");
				e.printStackTrace();
			}
		}

	}

	static String appendText(String line, String text) {
		// if the line is empty, append \n so it will become an enter in the telegram
		// bot, otherwise append it with the text of that line
		if ((checkMatch(line, "^$"))) {
			text = text + "\\n";
		} else {
			// Remove styling elements, <textarea>, <img>, comments and create bullet or
			// numbered lists
			if ((checkMatch(line, "<textarea>.*</textarea>"))) {
				text = text + line.replaceAll("<textarea>.*</textarea>", "");
				openQuestion = true;
			} else if ((checkMatch(line, "\\(.*\\)\\[(.*)\\]"))) {
				String lineText = returnGroup(line, "\\(.*\\)\\[(.*)\\]"); // just get the text and ignore styling
				text = text + lineText;
			} else if ((checkMatch(line, "0."))) {
				// replace the 0. used in .twee for lists by \n - \t for a list in the bot
				text = text + "\\n " + countList + ".\\t" + line.replaceAll("0.", "");
				countList++;
			} else if ((checkMatch(line, "\\* "))) {
				// bullet lists
				text = text + "\\n - \\t" + line.replaceAll("\\* ", "");
			} else if ((checkMatch(line, "\\[\\[(.*?)\\]\\]"))) {
				text = text + line;// .replaceAll(" ", "_").toLowerCase();
			} else if (checkMatch(line, "<!--.*-->")) {
				// do nothing for commented text
			} else if (checkMatch(line, "<img.*>")) {
				// delete the line for images for now
				// insert code in the future here if images can be send via Telegram
			} else {
				text = text + line;
			}
		}
		text = text.replaceAll("\\/\\/|\\'\\'|\\^\\^|~~", ""); // remove all bold, italic and strikethrough
		return text;

	}

	// --------- supportive functions ----------------

	// return a String of the first group
	static String returnGroup(String line, String regex) {
		Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(line);
		if (matcher.find()) {
			return matcher.group(1);
		} else {
			return "";
		}
	}

	static String appendAndReturnLinkName(String line, String regex, String regex2, String key) {
		Pattern pattern = Pattern.compile("\\[\\[(.*?->.*?)\\]\\]");
		Matcher matcher = pattern.matcher(line);
		// String linkName = "";
		while (matcher.find()) {
			// Get the matched text
			String match = matcher.group();
			// Extract the linkName and store it in lowercase
			String linkName = returnGroup(match, regex).replaceAll(".*\\]\\]", "").replaceAll("\\[\\[", "")
			        .replaceFirst("\\\\n", "");

			String next = returnGroup(match, regex2);

			keys.add(key);
			linkNames.add(linkName.toLowerCase().trim());
			goTos.add(next.toLowerCase().trim());
			line = line.replaceFirst(Pattern.quote(match), "[[" + linkName + "]]");

		}
		return line;
	}

	static String replaceVariables(String line) {
		Pattern pattern = Pattern.compile("<%= s.(.*)%>");
		Matcher matcher = pattern.matcher(line);
		// String linkName = "";
		while (matcher.find()) {
			// Get the matched text
			String match = matcher.group();
			// Extract the linkName and store it in lowercase
			String variable = returnGroup(match, "<%= s.(.*)%>").trim().toLowerCase();
			/// variableNames.add(variable);
			line = line.replaceFirst(Pattern.quote(match), " \"+ profile." + variable + "+\"");

		}
		// logger.info(line);
		return line;
	}

	static String defineVariable(String line, String key) {
		Pattern pattern = Pattern.compile("<%.*;.*%>");
		Matcher matcher = pattern.matcher(line);
		// String linkName = "";
		while (matcher.find()) {
			// Get the matched text
			String match = matcher.group();
			// Extract the linkName and store it in lowercase
			String varName = returnGroup(match, "<%.*s.(.*)=.*;.*%>").trim().toLowerCase();
			String varValue = returnGroup(match, "<%.*=(.*);.*%>").trim().toLowerCase().replace("\\\"", "");
			variableNames.add(varName);
			variableValues.add(varValue);
			keysVar.add(key);
			line = line.replaceFirst(Pattern.quote(match), "");

		}
		// logger.info(line);

		return line;
	}

	// return the text within [[]], determines the next panel
	// Only return it when there is one option, otherwise return empty String
	static String returnNext(String line, String regex) {
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(line);
		String n = "";
		int count = 0;
		while (matcher.find()) {
			count++;
			if (count == 1) {
				n = matcher.group(1);
			} else {
				n = "";
			}
		}
		return n;
	}

	// Return if the there is a match in that line based on a given regex.
	static boolean checkMatch(String line, String regex) {
		Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(line);
		boolean match = matcher.find();
		return match;
	}

}
