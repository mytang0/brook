package xyz.mytang0.brook.common.metadata.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class User implements Serializable {

    private static final long serialVersionUID = -4400011988863816617L;

    private String id;

    private String name;

    private String nickName;

    private String jobNumber;

    private String position;

    private String email;

    private String mobile;

    private List<String> roleIds;
}
