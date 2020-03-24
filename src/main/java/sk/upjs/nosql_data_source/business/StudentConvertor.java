package sk.upjs.nosql_data_source.business;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import de.undercouch.bson4jackson.BsonFactory;
import sk.upjs.nosql_data_source.entity.SimpleStudent;
import sk.upjs.nosql_data_source.entity.Student;
import sk.upjs.nosql_data_source.entity.StudijnyProgram;
import sk.upjs.nosql_data_source.entity.Studium;
import sk.upjs.nosql_data_source.persist.DaoFactory;
import sk.upjs.nosql_data_source.persist.StudentDao;

public class StudentConvertor {
	
	private List<Student> getStudents() {
		StudentDao dao = DaoFactory.INSTANCE.getStudentDao();
		return dao.getAll();
	}
	
	private String getJsonFromObject(Object o, boolean format) throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(SerializationFeature.INDENT_OUTPUT, format);
		return mapper.writeValueAsString(o);
	}
	
	public List<String> getStudentsJSON(boolean format) throws JsonProcessingException {
		List<Student> students = getStudents();
		List<String> result = new ArrayList<String>();
		for (Student student: students) {
			result.add(getJsonFromObject(student, format));
		}
		return result;
	}
	
	private byte[] getBSONFromObject(Object o) throws JsonGenerationException, JsonMappingException, IOException {
		BsonFactory bsonFactory = new BsonFactory();
		ObjectMapper mapper = new ObjectMapper(bsonFactory);
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		mapper.writeValue(outputStream, o);
		return outputStream.toByteArray();
	}
	
	public List<byte[]> getStudentsBSON() throws IOException {
		List<Student> students = getStudents();
		List<byte[]> result = new ArrayList<byte[]>();
		for (Student student: students) {
			result.add(getBSONFromObject(student));
		}
		return result;
	}
	
	private String getXMLFromObject(Object o, boolean format) throws JsonProcessingException {
		XmlMapper mapper = new XmlMapper();
		mapper.configure(SerializationFeature.INDENT_OUTPUT, format);
		return mapper.writeValueAsString(o);
	}
	
	public List<String> getStudentsXML(boolean format) throws JsonProcessingException {
		List<Student> students = getStudents();
		List<String> result = new ArrayList<String>();
		for (Student student: students) {
			result.add(getXMLFromObject(student, format));
		}
		return result;
	}
	
	private String getYAMLFromObject(Object o, boolean format) throws JsonProcessingException {
		YAMLMapper mapper = new YAMLMapper();
		mapper.configure(SerializationFeature.INDENT_OUTPUT, format);
		return mapper.writeValueAsString(o);
	}
	
	public List<String> getStudentsYAML(boolean format) throws JsonProcessingException {
		List<Student> students = getStudents();
		List<String> result = new ArrayList<String>();
		for (Student student: students) {
			result.add(getYAMLFromObject(student, format));
		}
		return result;
	}
	
	private String getCSVFromObject(Object o, Class objectClass, boolean withHeader) throws JsonProcessingException {
		CsvMapper mapper = new CsvMapper();
		CsvSchema schema = mapper.schemaFor(objectClass);
		if (withHeader)
			schema = schema.withHeader();
		ObjectWriter objectWriter = mapper.writer(schema);
		return objectWriter.writeValueAsString(o);
	}
	
	public String getSimpleStudentsCSV(boolean withHeader) throws JsonProcessingException {
		List<SimpleStudent> simpleStudents = DaoFactory.INSTANCE.getStudentDao().getSimpleStudents();
		return getCSVFromObject(simpleStudents, SimpleStudent.class, withHeader);
	}
	
	
	public static void main(String[] args) throws Throwable {
		StudentConvertor sc = new StudentConvertor();
//		List<String> students = sc.getStudentsJSON(false);
//		for (String s: students) {
//			System.out.println(s);			
//		}
		
//		List<byte[]> studentsBytes = sc.getStudentsBSON();
//		System.out.println(new String(studentsBytes.get(0)));
		
//		List<String> students = sc.getStudentsXML(true);
//		for (String s: students) {
//			System.out.println(s);			
//		}
		
//		List<String> students = sc.getStudentsYAML(false);
//		for (String s: students) {
//			System.out.println(s);			
//		}
		
		String csvStudents = sc.getSimpleStudentsCSV(true);
		System.out.println(csvStudents);	
		
//		List<Student> students = sc.getStudents();
//		List<StudijnyProgram> studijneProgramy = new ArrayList<StudijnyProgram>();
//		for (Student student: students) {
//			for (Studium studium: student.getStudium()) {
//				studijneProgramy.add(studium.getStudijnyProgram());
//			}
//		}
//		String csvStudijneProgramy = sc.getCSVFromObject(studijneProgramy, StudijnyProgram.class, true);
//		System.out.println(csvStudijneProgramy);
	}
	
}
