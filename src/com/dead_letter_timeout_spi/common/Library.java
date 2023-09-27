package com.dead_letter_timeout_spi.common;

import static dead_letter_timeout_spi.Dead_letter_timeout_SPI.FW;
import java.io.IOException;
import java.io.StringReader;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class Library {

    public static String getDate(Boolean withTime, Boolean withDivisions, Boolean withMilli, Boolean useSpecialChars, String timezone) {
        try {
            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
            OffsetDateTime nowTruncated = now.truncatedTo(ChronoUnit.MILLIS);

            if (!timezone.equals("UTC"))
                nowTruncated = nowTruncated.minusHours(3);

            String format = createDateFormat(withTime, withDivisions, withMilli, useSpecialChars);
            nowTruncated.format(DateTimeFormatter.ofPattern(format));

            String result = nowTruncated.toString();
            if (withTime && withDivisions && withMilli && useSpecialChars && result.length() == 20) {
                result = result.substring(0, 19) + ".000Z";
            }

            return result;
        } catch (Exception ex) {
            FW.writeException(ex);
            return null;
        }
    }

    public static int getDay() {
        try {
            ZonedDateTime currentSystemTime = ZonedDateTime.now();
            ZonedDateTime currentUTCTime = currentSystemTime.withZoneSameInstant(ZoneOffset.UTC);
            return currentUTCTime.getDayOfMonth();
        } catch (Exception ex) {
            FW.writeException(ex);
            return -1;
        }
    }

    public static int getMonth() {
        try {
            ZonedDateTime currentSystemTime = ZonedDateTime.now();
            ZonedDateTime currentUTCTime = currentSystemTime.withZoneSameInstant(ZoneOffset.UTC);
            return currentUTCTime.getMonthValue();
        } catch (Exception ex) {
            FW.writeException(ex);
            return -1;
        }
    }

    public static int getYear() {
        try {
            ZonedDateTime currentSystemTime = ZonedDateTime.now();
            ZonedDateTime currentUTCTime = currentSystemTime.withZoneSameInstant(ZoneOffset.UTC);
            return currentUTCTime.getYear();
        } catch (Exception ex) {
            FW.writeException(ex);
            return -1;
        }
    }

    private static String createDateFormat(Boolean withTime, Boolean withDivisions, Boolean withMilliseconds, Boolean useSpecialChars) {
        try {
            String format;
            if (withTime && withDivisions && withMilliseconds && useSpecialChars) {
                format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
            } else if (withTime && withDivisions && withMilliseconds && !useSpecialChars) {
                format = "yyyy-MM-dd HH:mm:ss.SSS";
            } else if (withTime && withDivisions && !withMilliseconds) {
                format = "yyyy-MM-dd HH:mm:ss";
            } else if (withTime && !withDivisions && withMilliseconds) {
                format = "yyyyMMddHHmmssSSS";
            } else if (withTime && !withDivisions && !withMilliseconds) {
                format = "yyyyMMddHHmmss";
            } else if (!withTime && withDivisions) {
                format = "yyyy-MM-dd";
            } else if (!withTime && !withDivisions) {
                format = "yyyyMMdd";
            } else {
                format = "yyyy-MM-dd HH:mm:ss.SSS";
            }

            return format;
        } catch (Exception ex) {
            FW.writeException(ex);
            return null;
        }
    }

    public static void sleep(Integer milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException ex) {
            FW.writeException(ex);
        }
    }

    public static String removeQuebrasLinha(String original) {
        try {
            return original.replaceAll("[\r\n]+", "");
        } catch (Exception ex) {
            FW.writeException(ex);
            return null;
        }
    }

    public static Document xmlStringToDocument(String xml) {
        try {
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            InputSource src = new InputSource();
            src.setCharacterStream(new StringReader(xml));
            Document doc = builder.parse(src);
            return doc;
        } catch (IOException | ParserConfigurationException | SAXException ex) {
            FW.writeException(ex);
            return null;
        }
    }

    public static String getStringFromJson(JSONObject json, String target) {
        try {
            return json.has(target) ? (String) json.get(target) : null;
        } catch (Exception ex) {
            FW.writeException(ex);
            return null;
        }
    }

    public static String generateRandomAlphanumericString() {
        try {
            int size = 23;
            int leftLimit = 48;
            int rightLimit = 122;
            Random random = new Random();

            return random.ints(leftLimit, rightLimit + 1)
                    .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                    .limit(size)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                    .toString();
        } catch (Exception ex) {
            FW.writeException(ex);
            return null;
        }
    }

    public static boolean isGUIDValid(String input) {
        try {
            String guidPattern = "^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$";
            return Pattern.matches(guidPattern, input);
        } catch (Exception ex) {
            FW.writeException(ex);
            return false;
        }
    }

    public static org.bson.Document getIndex(int order) {
        try {
            org.bson.Document index = new org.bson.Document("_id", order);
            return index;
        } catch (Exception ex) {
            FW.writeException(ex);
            return null;
        }
    }

    public static org.bson.Document getDefaultSort(int order) {
        try {
            org.bson.Document sort = new org.bson.Document("_id", order);
            return sort;
        } catch (Exception ex) {
            FW.writeException(ex);
            return null;
        }
    }
}
