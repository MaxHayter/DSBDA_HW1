package bdtc.utils;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Screen area
 */
@AllArgsConstructor
public class Area {

    /**
     * Upper-left corner of the area
     */
    @Getter
    private final Pair begin;

    /**
     * Lower-right corner of the area
     */
    @Getter
    private final Pair end;

}
