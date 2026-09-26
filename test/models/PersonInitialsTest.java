package models;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class PersonInitialsTest {

	@Test
	public void testStandardNames() {
		Person p1 = new Person();
		p1.setFirstname("John");
		p1.setLastname("Doe");
		assertEquals("JD", p1.getInitials());

		Person p2 = new Person();
		p2.setFirstname("Ada");
		p2.setLastname("Lovelace");
		assertEquals("AL", p2.getInitials());
	}

	@Test
	public void testDutchAndComplexPrefixes() {
		Person p1 = new Person();
		p1.setFirstname("Jan");
		p1.setLastname("van der Linden");
		assertEquals("JL", p1.getInitials());

		Person p2 = new Person();
		p2.setFirstname("Piet");
		p2.setLastname("de Boer");
		assertEquals("PB", p2.getInitials());

		Person p3 = new Person();
		p3.setFirstname("Anna");
		p3.setLastname("van Dijk");
		assertEquals("AD", p3.getInitials());

		Person p4 = new Person();
		p4.setFirstname("Dirk");
		p4.setLastname("van den Berg");
		assertEquals("DB", p4.getInitials());
	}

	@Test
	public void testPrefixOnlyLastNames() {
		// When last name consists only of a prefix, it should not throw and should retain the initial
		Person p1 = new Person();
		p1.setFirstname("Jan");
		p1.setLastname("de");
		assertEquals("JD", p1.getInitials());

		Person p2 = new Person();
		p2.setFirstname("Jan");
		p2.setLastname("de ");
		assertEquals("JD", p2.getInitials());

		Person p3 = new Person();
		p3.setFirstname("Jan");
		p3.setLastname("van der");
		assertEquals("JD", p3.getInitials());
	}

	@Test
	public void testMissingOrWhitespaceLastNames() {
		// These previously threw java.lang.StringIndexOutOfBoundsException
		Person p1 = new Person();
		p1.setFirstname("Alice");
		p1.setLastname(null);
		assertEquals("A", p1.getInitials());

		Person p2 = new Person();
		p2.setFirstname("Alice");
		p2.setLastname("");
		assertEquals("A", p2.getInitials());

		Person p3 = new Person();
		p3.setFirstname("Alice");
		p3.setLastname("   ");
		assertEquals("A", p3.getInitials());

		Person p4 = new Person();
		p4.setFirstname("Mary Jane");
		p4.setLastname("");
		assertEquals("MJ", p4.getInitials());
	}

	@Test
	public void testMissingOrWhitespaceFirstNames() {
		Person p1 = new Person();
		p1.setFirstname(null);
		p1.setLastname("Smith");
		assertEquals("S", p1.getInitials());

		Person p2 = new Person();
		p2.setFirstname("");
		p2.setLastname("Smith");
		assertEquals("S", p2.getInitials());

		Person p3 = new Person();
		p3.setFirstname("   ");
		p3.setLastname("Smith");
		assertEquals("S", p3.getInitials());

		Person p4 = new Person();
		p4.setFirstname("");
		p4.setLastname("van der Linden");
		assertEquals("L", p4.getInitials());
	}

	@Test
	public void testFallbackToEmail() {
		Person p1 = new Person();
		p1.setFirstname(null);
		p1.setLastname(null);
		p1.setEmail("alice@example.org");
		assertEquals("A", p1.getInitials());

		Person p2 = new Person();
		p2.setFirstname("");
		p2.setLastname("   ");
		p2.setEmail("bob@example.org");
		assertEquals("B", p2.getInitials());
	}

	@Test
	public void testCompletelyEmptyPerson() {
		Person p1 = new Person();
		p1.setFirstname(null);
		p1.setLastname(null);
		p1.setEmail(null);
		assertEquals("", p1.getInitials());

		Person p2 = new Person();
		p2.setFirstname("   ");
		p2.setLastname("   ");
		p2.setEmail("   ");
		assertEquals("", p2.getInitials());
	}

	@Test
	public void testSingleCharacterAndHyphenatedNames() {
		Person p1 = new Person();
		p1.setFirstname("A");
		p1.setLastname("B");
		assertEquals("AB", p1.getInitials());

		Person p2 = new Person();
		p2.setFirstname("Jean-Luc");
		p2.setLastname("Picard");
		assertEquals("JP", p2.getInitials());
	}
}
