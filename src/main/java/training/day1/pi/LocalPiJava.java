package training.day1.pi;

import static java.lang.Math.random;

public class LocalPiJava {
    public static void main(String[] args) {
        int count = 0;
        for (int i = 0; i < 100000; i++) {
            double x = random() * 2 - 1;
            double y = random() * 2 - 1;
            if (x * x + y * y < 1) count += 1;
        }
        System.out.println("Pi is roughly " + 4 * count / 100000.0);
    }
}
