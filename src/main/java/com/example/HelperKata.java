package com.example;


import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

//

public class HelperKata {
    private static final String EMPTY_STRING = "";
    private static String ANTERIOR_BONO = null;

    private static Flux<String> createFluxFrom(String fileBase64) {
        return Flux.using(
                () -> new BufferedReader(
                        new InputStreamReader(
                                new ByteArrayInputStream(decodeBase64(fileBase64))))
                        .lines(),
                Flux::fromStream,
                Stream::close
        );
    }

    public static Flux<CouponDetailDto> getListFromBase64File(final String fileBase64) {
        return getCouponDetailDtoFlux(createFluxFrom(fileBase64));
    }

    private static Flux<CouponDetailDto> getCouponDetailDtoFlux(Flux<String> fileFlux) {
        AtomicInteger counter = new AtomicInteger(0);
        String characterSeparated = FileCSVEnum.CHARACTER_DEFAULT.getId();
        Set<String> codes = new HashSet<>();
        return fileFlux.skip(1)
                .map(line -> getTupleOfLine(line, line.split(characterSeparated), characterSeparated))
                .map(tuple -> getCouponDetailDto(counter, codes, tuple));
    }

    private static CouponDetailDto getCouponDetailDto(AtomicInteger counter, Set<String> codes, Tuple2<String, String> tuple) {
        String dateValidated;
        String bonoForObject;
        String bonoEnviado;

        String errorMessage = errorMessage(codes, tuple);
        dateValidated = Optional.of(errorMessage).filter(el -> el.equals(null)).map(el -> tuple.getT2()).orElse(null);
        bonoEnviado = tuple.getT1();
        bonoForObject = getBonoForObject(bonoEnviado);

        return CouponDetailDto.aCouponDetailDto()
                .withCode(bonoForObject)
                .withDueDate(dateValidated)
                .withNumberLine(counter.incrementAndGet())
                .withMessageError(errorMessage)
                .withTotalLinesFile(1)
                .build();
    }

    private static String errorMessage(Set<String> codes, Tuple2<String, String> tuple){
        Map<String,Boolean> map = new LinkedHashMap();
        map.put(ExperienceErrorsEnum.FILE_ERROR_COLUMN_EMPTY.toString(),isBlank(tuple));
        map.put(ExperienceErrorsEnum.FILE_ERROR_CODE_DUPLICATE.toString(),!codes.add(tuple.getT1()));
        map.put(ExperienceErrorsEnum.FILE_ERROR_DATE_PARSE.toString(),!validateDateRegex(tuple.getT2()));
        map.put(ExperienceErrorsEnum.FILE_DATE_IS_MINOR_OR_EQUALS.toString(),validateDateIsMinor(tuple.getT2()));

        for (Map.Entry<String, Boolean> jugador : map.entrySet()){
            if(jugador.getValue()){
                return jugador.getKey();
            }
        }
        return null;
    }

    private static boolean isBlank(Tuple2<String, String> tuple) {
        return tuple.getT1().isBlank() || tuple.getT2().isBlank();
    }

    private static String getBonoForObject(String bonoEnviado) {
        String bonoForObject = null;
        if (isNullOrEquals(bonoEnviado)) {
            ANTERIOR_BONO = typeBono(bonoEnviado);
            bonoForObject = bonoEnviado;
        }
        return bonoForObject;
    }

    private static boolean isNullOrEquals(String bonoEnviado) {
        return ANTERIOR_BONO == null || ANTERIOR_BONO.equals(typeBono(bonoEnviado));
    }

    public static String typeBono(String bonoIn) {
        String retorno;

        retorno = Optional.of(bonoIn).filter(HelperKata::matchesBono)
                .map(el -> ValidateCouponEnum.EAN_13
                        .getTypeOfEnum()).toString();
        retorno = Optional.of(bonoIn)
                .filter(HelperKata::isBooleanReplaceAsteriscos)
                .map(el -> ValidateCouponEnum.EAN_39.getTypeOfEnum())
                .orElse(ValidateCouponEnum.ALPHANUMERIC.getTypeOfEnum());
        return retorno;
    }

    private static boolean isBooleanReplaceAsteriscos(String bonoIn) {
        return bonoIn.startsWith("*")
                && lenghtAsteriscos(bonoIn);
    }

    private static boolean lenghtAsteriscos(String bonoIn) {
        return numberBiggestOrEqual(bonoIn.replace("*", "").length(), 1)
                && numberLessOrEqual(bonoIn.replace("*", "").length(), 43);
    }

    private static boolean numberLessOrEqual(int num1, int num2) {
        return num1 <= num2;
    }

    private static boolean numberBiggestOrEqual(int num1, int num2) {
        return num1 >= num2;
    }

    private static boolean matchesBono(String bonoIn) {
        return bonoIn.chars().allMatch(Character::isDigit) && lenghtBono(bonoIn);
    }

    private static boolean lenghtBono(String bonoIn) {
        return numberBiggestOrEqual(bonoIn.length(), 12) && numberLessOrEqual(bonoIn.length(), 13);
    }

    public static boolean validateDateRegex(String dateForValidate) {
        String regex = FileCSVEnum.PATTERN_DATE_DEFAULT.getId();
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(dateForValidate);
        return matcher.matches();
    }

    private static byte[] decodeBase64(final String fileBase64) {
        return Base64.getDecoder().decode(fileBase64);

    }

    private static Tuple2<String, String> getTupleOfLine(String line, String[] array, String characterSeparated) {
        return condicionprincipal(array)
                ? Tuples.of(EMPTY_STRING, EMPTY_STRING)
                : ternario2(line, array, characterSeparated);
    }

    private static boolean condicionprincipal(String[] array) {
        return Objects.isNull(array) || array.length == 0;
    }

    private static Tuple2<String, String> ternario2(String line, String[] array, String characterSeparated) {
        return array.length < 2
                ? ternario3(line, array, characterSeparated)
                : Tuples.of(array[0], array[1]);
    }

    private static Tuple2<String, String> ternario3(String line, String[] array, String characterSeparated) {
        return line.startsWith(characterSeparated)
                ? Tuples.of(EMPTY_STRING, array[0])
                : Tuples.of(array[0], EMPTY_STRING);
    }

    public static boolean validateDateIsMinor(String dateForValidate) {
        Boolean retorno = false;
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(FileCSVEnum.PATTERN_SIMPLE_DATE_FORMAT.getId());
            Date dateActual = simpleDateFormat.parse(simpleDateFormat.format(new Date()));
            Date dateCompare = simpleDateFormat.parse(dateForValidate);
            retorno = numberLessOrEqual(dateCompare.compareTo(dateActual), 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retorno;
    }

}
