package utils.tools;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import com.google.zxing.BarcodeFormat;
import com.google.zxing.MultiFormatWriter;
import com.google.zxing.WriterException;
import com.google.zxing.client.j2se.MatrixToImageWriter;
import com.google.zxing.common.BitMatrix;

import jakarta.inject.Inject;
import play.cache.SyncCacheApi;
import play.libs.Files;
import play.libs.Files.TemporaryFile;

public class QRCodeUtil {

	@Inject
	SyncCacheApi cache;

	/**
	 * generate QR code to <code>data</code>, and cache it with <code>key</code> and for <code>timeout</code> seconds
	 * 
	 * @param data
	 * @param key
	 * @param timeout
	 * @return
	 */
	public String generateCachedQRCode(String data, String key, int timeout) {
		return cache.getOrElseUpdate(key, () -> {
			return generateQRCode(data);
		}, timeout);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private String generateQRCode(String data) throws WriterException, IOException {
		TemporaryFile tf = Files.singletonTemporaryFileCreator().create(System.currentTimeMillis() + "_qrcode", ".png");
		generateQRcode(data, tf.path(), StandardCharsets.UTF_8, 256, 256);
		return tf.path().toAbsolutePath().toString();
	}

	private void generateQRcode(String data, Path path, Charset charset, int h, int w)
	        throws WriterException, IOException {
		// the BitMatrix class represents the 2D matrix of bits
		// MultiFormatWriter is a factory class that finds the appropriate Writer subclass for the BarcodeFormat
		// requested and encodes the barcode with the supplied contents.
		BitMatrix matrix = new MultiFormatWriter().encode(new String(data.getBytes(charset), charset),
		        BarcodeFormat.QR_CODE, w, h);
		MatrixToImageWriter.writeToPath(matrix, "png", path);
	}

}
