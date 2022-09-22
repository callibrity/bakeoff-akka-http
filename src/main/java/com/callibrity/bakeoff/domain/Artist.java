package com.callibrity.bakeoff.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode(of = "id")
@AllArgsConstructor
@NoArgsConstructor
public class Artist {

// ------------------------------ FIELDS ------------------------------

    String id;
    String name;
    Genre genre;

}
