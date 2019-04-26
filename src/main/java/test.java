/**
 * @author jiale.he
 * @date 2019/03/05
 * @email jiale.he@mail.hypers.com
 */
public class test {
    public static void main(String[] args) {
        //int length = findLength("heeeale", "jeele");
        System.out.println(LCS("hejiale","sdadejiaaa"));
    }

    public static String LCS(String stringA, String stringB) {
        if(stringA==null || stringB==null){
            return null;
        }
        if(stringA.length()<1 || stringB.length()<1){
            return "";
        }
        if (stringA.contains(stringB)) {
            return stringB;
        }
        else if (stringB.length() == 1) {
            return "";
        }

        String leftSerach = LCS(stringA, stringB.substring(0, stringB.length() - 1));
        String rightSerach = LCS(stringA, stringB.substring(1, stringB.length()));
        return leftSerach.length() >= rightSerach.length() ? leftSerach : rightSerach;
    }

    public static int findLength(String str1,String str2) {
        char[] str1s = str1.toCharArray();
        char[] str2s = str2.toCharArray();
        int maxLen = 0;
        char[][] dp = new char[str1s.length][str2s.length];
        for (int i = 0; i < str1s.length; i++) {
            for (int j = 0; j < str2s.length; j++) {
                if (str1s[i] == str2s[j]) {
                    if (i != 0 && j != 0) {
                        dp[i][j] = (char) (dp[i - 1][j - 1] + 1);
                        if (dp[i][j] > maxLen) {
                            maxLen = dp[i][j];
                        }
                    } else {
                        dp[i][j] = 1;
                    }
                }
            }
        }
        return maxLen;
    }
}
