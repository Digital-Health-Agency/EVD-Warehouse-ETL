{% macro uhai_point_of_entry_name(column) %}
    case lower(trim({{ column }}))
        when 'jkia' then 'Jomo Kenyatta International Airport'
        when 'busia' then 'Busia OSBP'
        when 'mia' then 'Moi International Airport'
        when 'lwakhakha' then 'Lwakhakha Border Post'
        when 'eia_eldoret' then 'Eldoret International Airport'
        when 'kia_kisumu' then 'Kisumu International Airport'
        when 'isebania' then 'Isebania OSBP'
        when 'wajir_airport' then 'Wajir Airport'
        when 'malaba' then 'Malaba OSBP'
        when 'isiolo_airport' then 'Isiolo International Airport'
        when 'wap' then 'Wilson International Airport'
        when 'namanga' then 'Namanga OSBP'
        when 'mbita' then 'Mbita'
        when 'suam' then 'Suam Border Post'
        when 'malindi_airport' then 'Malindi Airport'
        when 'old_port' then 'Old Seaport'
        when 'muhuru_bay' then 'Muhuru-bay'
        when 'nadapal' then 'Nadapal Border Post'
        when 'lungalunga' then 'Lunga-lunga OSBP'
        when 'moyale' then 'Moyale OSBP'
        when 'kilindini' then 'Kilindini Sea Port'
        when 'taveta' then 'Taveta OSBP'
        else initcap(replace(lower(nullif(trim({{ column }}), '')), '_', ' '))
    end
{% endmacro %}
