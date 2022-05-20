package org.apache.compactatlas.example;

import org.springframework.security.crypto.bcrypt.BCrypt;

public class GeneratePasswdHash {
    public static void main(String[] args) {
        String salt = BCrypt.gensalt();
        String hashedpw = BCrypt.hashpw("guest",salt);
        System.out.println(hashedpw);
        System.out.println(BCrypt.checkpw("guest", hashedpw));
    }
}
