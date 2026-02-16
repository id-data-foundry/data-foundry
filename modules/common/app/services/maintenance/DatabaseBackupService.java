package services.maintenance;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Files;
import com.google.inject.Singleton;

import datasets.DatasetConnector;
import io.ebean.DB;
import io.ebean.Transaction;
import models.Project;
import models.ds.LinkedDS;
import play.Logger;
import play.libs.Json;
import services.inlets.ScheduledService;

@Singleton
public class DatabaseBackupService implements ScheduledService {

	private final DatasetConnector datasetConnector;

	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	private final SimpleDateFormat sdfhm = new SimpleDateFormat("yyyyMMdd_HHmm");
	private static final Logger.ALogger logger = Logger.of(DatabaseBackupService.class);

	private final Semaphore backupSemaphore = new Semaphore(1);
	private final Semaphore dbStatsSemaphore = new Semaphore(1);

	@Inject
	public DatabaseBackupService(DatasetConnector datasetConnector) {
		this.datasetConnector = datasetConnector;
	}

	@Override
	public void refresh() {
		logger.info("Nightly DB backup amd stats run...");
		performBackup(sdf.format(new Date()));
		performDatabaseStats(sdf.format(new Date()));
	}

	public void adminBackup() {
		logger.info("Creating on-demand DB backup for admin...");
		performBackup(sdfhm.format(new Date()) + "_admin");
	}

	public void adminDBStats() {
		logger.info("Creating on-demand DB stats for admin...");
		performDatabaseStats(sdfhm.format(new Date()) + "_admin");
	}

	private void performBackup(String dateString) {
		try {
			if (backupSemaphore.tryAcquire()) {
				// zip db
				try (Transaction t = DB.beginTransaction(); Connection conn = t.connection();) {
					String dbBackupLocation = "" + dateString + "_db.zip";
					CallableStatement csbu = conn.prepareCall("BACKUP TO '" + dbBackupLocation + "';");
					csbu.execute();
					logger.info("Database backup: " + new File(dbBackupLocation).getAbsolutePath());
					t.commit();
				} catch (SQLException e) {
					logger.error("Database backup failed.", e);
				}

			}
		} catch (Exception e) {
			logger.error("Problem with admin backup", e);
		} finally {
			backupSemaphore.release();
		}
	}

	private void performDatabaseStats(String dateString) {
		try {
			if (dbStatsSemaphore.tryAcquire()) {
				final ArrayNode an = Json.newArray();
				try {
					final List<Project> projects = Project.find.all();
					projects.forEach(project -> {

						// don't touch frozen projects here
						if (project.isFrozen()) {
							return;
						}

						ObjectNode on = an.addObject();

						// collect stats for project
						on.put("id", project.getId());
						on.put("name", project.getName());
						on.put("owner", project.getOwner().getName());
						on.put("active", project.isActive());
						on.put("participants", project.getParticipants().size());
						on.put("devices", project.getDevices().size());
						on.put("wearables", project.getWearables().size());

						// collect stats for project datasets
						ArrayNode datasets = on.putArray("datasets");
						datasets.addAll(
						        project.getDatasets().stream().filter(ds -> !ds.getRefId().isEmpty()).map(ds -> {
							        LinkedDS lds = datasetConnector.getDatasetDS(ds);
							        return lds.stats();
						        }).collect(Collectors.toSet()));
					});

					// write out stats to a daily file
					String dbBackupLocation = dateString + "_stats.json";
					Files.write(an.toString().getBytes(StandardCharsets.UTF_8), new File(dbBackupLocation));

					// and to a rolling file for stats display
					String currentBackupLocation = "current_stats.json";
					Files.write(an.toString().getBytes(StandardCharsets.UTF_8), new File(currentBackupLocation));
				} catch (IOException e) {
					logger.error("daily database stats", e);
				} catch (Exception e) {
					logger.error("daily database stats", e);
				}
			}
		} catch (Exception e) {
			logger.error("Problem with admin backup", e);
		} finally {
			dbStatsSemaphore.release();
		}
	}

	@Override
	public void stop() {
	}

}
