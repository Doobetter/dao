package org.roc.utils;

import java.util.BitSet;

import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;

public class EmailServiceUtil {
	public static void test() throws EmailException{
		Email email = new SimpleEmail();
		email.setHostName("smtp.163.com");
		email.setSmtpPort(465);
		email.setAuthenticator(new DefaultAuthenticator("lc464297691@163.com", "juanjuan0428"));
		email.setSSLOnConnect(true);
		email.setFrom("lc464297691@163.com");
		email.setSubject("TestMail");
		email.setMsg("This is a test mail ... :-)");
		email.addTo("doobetter@163.com");
		email.send();
	}
	
	public static void main(String[] args) throws EmailException {
		BitSet set = new BitSet();
		set.set(1);set.set(0);set.set(256);
		long [] a = set.toLongArray();
		for(int i = 0 ; i < a.length; i++) System.out.println(a[i]);
		System.out.println(set.toString());
	}
}
