package utils.validators;

abstract public class AbstractValidator {
	abstract public boolean validate(String value);

	/**
	 * explain positive validation result
	 * 
	 * @return
	 */
	public String explainYes(String key, String value) {
		return "";
	}

	/**
	 * explain negative validation result
	 * 
	 * @return
	 */
	public String explainNo(String key, String value) {
		return "";
	}
}
