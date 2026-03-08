import java.sql.*;

public class MySQLtoSQLServerTransfer {
    public static void main(String[] args) {
        try {
           
            Connection mysqlConnection = DriverManager.getConnection(
                    "jdbc:mysql://51.91.220.146:3306/ozeol_db", "DataOzeol", "OOlàMPOz00T@)");

           
            Connection sqlServerConnection = DriverManager.getConnection(
                    "jdbc:sqlserver://SRVCOGNOSDEV;encrypt=true;trustServerCertificate=true;databaseName=STAGING", "sa", "GgBIzO3OKjJArL");

            
            String mysqlQuery = "SELECT * FROM `ozeol_db`.`LOG_CRM` where cmk_s_field_datetraitement >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);";
            Statement mysqlStatement = mysqlConnection.createStatement();
            ResultSet resultSet = mysqlStatement.executeQuery(mysqlQuery);

            
            String sqlServerInsertQuery = "INSERT INTO [dbo].[COM_LOG_CRM_CLOUD] (nom,tel1,tel2,tel3,fax1,fax2,fichier,nom_campagne,num_campagne,num_fichier,agent_nom,agent_login,num_agent,matricule_user,genre_user,code_externe_user,login_agent,agent_group_competence,num_contact,obs_c_num_import,num_qualification,last_date_fin,last_date_rappel,etat,obs_c_date_fin,obs_c_date_rappel,obs_c_date_debut,obs_c_duree,obs_c_duree_appel,obs_c_date_hangup,obs_c_tel,obs_c_poste,obs_c_observation,obs_c_dmatt,obs_c_dmc,obs_c_dmpa,obs_c_dmt,obs_c_dmprod,obs_c_caller_id,obs_c_qualif_tel1,obs_c_qualif_tel2,obs_c_qualif_tel3,obs_c_qualif_fax1,obs_c_qualif_fax2,date_qualif_tel1,date_qualif_tel2,date_qualif_tel3,date_qualif_fax1,date_qualif_fax2,last_num_observation_contact,commercial_nom,libelle_import,date_import,date_rdv,observation_rdv,etat_rdv,retour_rdv,COMMERCIAL_SUBJECT,COMMERCIAL_LOCATION,COMMERCIAL_DESCRIPTION,COMMERCIAL_STARTTIME,COMMERCIAL_ENDTIME,CMK_S_FIELD_QUALIFICATION,CMK_S_FIELD_CODE_QUALIFICATION,CMK_S_FIELD_QUALIFICATION_MERE,CMK_S_FIELD_CODE_QUALIFICATION_MERE,CMK_S_FIELD_NUM_QUALIFICATION,CMK_S_FIELD_QUALIF_TEL1,CMK_S_FIELD_QUALIF_TEL2,CMK_S_FIELD_QUALIF_TEL3,CMK_S_FIELD_QUALIF_FAX1,CMK_S_FIELD_QUALIF_FAX2,CMK_S_FIELD_DATE_QUALIF_TEL1,CMK_S_FIELD_DATE_QUALIF_TEL2,CMK_S_FIELD_DATE_QUALIF_TEL3,CMK_S_FIELD_DATE_QUALIF_FAX1,CMK_S_FIELD_DATE_QUALIF_FAX2,CMK_S_FIELD_DATEHEURETRAITEMENT,CMK_S_FIELD_DATETRAITEMENT,CMK_S_FIELD_HEURETRAITEMENT,CMK_S_FIELD_NOMCAMPAGNE,CMK_S_FIELD_NOMFICHIER,CMK_S_FIELD_COMMENTAIRES,CMK_S_FIELD_NUM_IMPORT,CMK_S_FIELD_AGENT_NOM,CMK_S_FIELD_AGENT_LOGIN,CMK_S_FIELD_COMMERCIAL_NOM,nums_observation_contact) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement sqlServerStatement = sqlServerConnection.prepareStatement(sqlServerInsertQuery);

            
            while (resultSet.next()) {
                sqlServerStatement.setString(1, resultSet.getString("nom"));
                sqlServerStatement.setString(2, resultSet.getString("tel1"));
                sqlServerStatement.setString(3, resultSet.getString("tel2"));
                sqlServerStatement.setString(4, resultSet.getString("tel3"));
                sqlServerStatement.setString(5, resultSet.getString("fax1"));
                sqlServerStatement.setString(6, resultSet.getString("fax2"));
                sqlServerStatement.setString(7, resultSet.getString("fichier"));
                sqlServerStatement.setString(8, resultSet.getString("nom_campagne"));
                sqlServerStatement.setString(9, resultSet.getString("num_campagne"));
                sqlServerStatement.setString(10, resultSet.getString("num_fichier"));
                sqlServerStatement.setString(11, resultSet.getString("agent_nom"));
                sqlServerStatement.setString(12, resultSet.getString("agent_login"));
                sqlServerStatement.setString(13, resultSet.getString("num_agent"));
                sqlServerStatement.setString(14, resultSet.getString("matricule_user"));
                sqlServerStatement.setString(15, resultSet.getString("genre_user"));
                sqlServerStatement.setString(16, resultSet.getString("code_externe_user"));
                sqlServerStatement.setString(17, resultSet.getString("login_agent"));
                sqlServerStatement.setString(18, resultSet.getString("agent_group_competence"));
                sqlServerStatement.setString(19, resultSet.getString("num_contact"));
                sqlServerStatement.setString(20, resultSet.getString("obs_c_num_import"));
                sqlServerStatement.setString(21, resultSet.getString("num_qualification"));
                sqlServerStatement.setString(22, resultSet.getString("last_date_fin"));
                sqlServerStatement.setString(23, resultSet.getString("last_date_rappel"));
                sqlServerStatement.setString(24, resultSet.getString("etat"));
                sqlServerStatement.setString(25, resultSet.getString("obs_c_date_fin"));
                sqlServerStatement.setString(26, resultSet.getString("obs_c_date_rappel"));
                sqlServerStatement.setString(27, resultSet.getString("obs_c_date_debut"));
                sqlServerStatement.setString(28, resultSet.getString("obs_c_duree"));
                sqlServerStatement.setString(29, resultSet.getString("obs_c_duree_appel"));
                sqlServerStatement.setString(30, resultSet.getString("obs_c_date_hangup"));
                sqlServerStatement.setString(31, resultSet.getString("obs_c_tel"));
                sqlServerStatement.setString(32, resultSet.getString("obs_c_poste"));
                sqlServerStatement.setString(33, resultSet.getString("obs_c_observation"));
                sqlServerStatement.setString(34, resultSet.getString("obs_c_dmatt"));
                sqlServerStatement.setString(35, resultSet.getString("obs_c_dmc"));
                sqlServerStatement.setString(36, resultSet.getString("obs_c_dmpa"));
                sqlServerStatement.setString(37, resultSet.getString("obs_c_dmt"));
                sqlServerStatement.setString(38, resultSet.getString("obs_c_dmprod"));
                sqlServerStatement.setString(39, resultSet.getString("obs_c_caller_id"));
                sqlServerStatement.setString(40, resultSet.getString("obs_c_qualif_tel1"));
                sqlServerStatement.setString(41, resultSet.getString("obs_c_qualif_tel2"));
                sqlServerStatement.setString(42, resultSet.getString("obs_c_qualif_tel3"));
                sqlServerStatement.setString(43, resultSet.getString("obs_c_qualif_fax1"));
                sqlServerStatement.setString(44, resultSet.getString("obs_c_qualif_fax2"));
                sqlServerStatement.setString(45, resultSet.getString("date_qualif_tel1"));
                sqlServerStatement.setString(46, resultSet.getString("date_qualif_tel2"));
                sqlServerStatement.setString(47, resultSet.getString("date_qualif_tel3"));
                sqlServerStatement.setString(48, resultSet.getString("date_qualif_fax1"));
                sqlServerStatement.setString(49, resultSet.getString("date_qualif_fax2"));
                sqlServerStatement.setString(50, resultSet.getString("last_num_observation_contact"));
                sqlServerStatement.setString(51, resultSet.getString("commercial_nom"));
                sqlServerStatement.setString(52, resultSet.getString("libelle_import"));
                sqlServerStatement.setString(53, resultSet.getString("date_import"));
                sqlServerStatement.setString(54, resultSet.getString("date_rdv"));
                sqlServerStatement.setString(55, resultSet.getString("observation_rdv"));
                sqlServerStatement.setString(56, resultSet.getString("etat_rdv"));
                sqlServerStatement.setString(57, resultSet.getString("retour_rdv"));
                sqlServerStatement.setString(58, resultSet.getString("COMMERCIAL_SUBJECT"));
                sqlServerStatement.setString(59, resultSet.getString("COMMERCIAL_LOCATION"));
                sqlServerStatement.setString(60, resultSet.getString("COMMERCIAL_DESCRIPTION"));
                sqlServerStatement.setString(61, resultSet.getString("COMMERCIAL_STARTTIME"));
                sqlServerStatement.setString(62, resultSet.getString("COMMERCIAL_ENDTIME"));
                sqlServerStatement.setString(63, resultSet.getString("CMK_S_FIELD_QUALIFICATION"));
                sqlServerStatement.setString(64, resultSet.getString("CMK_S_FIELD_CODE_QUALIFICATION"));
                sqlServerStatement.setString(65, resultSet.getString("CMK_S_FIELD_QUALIFICATION_MERE"));
                sqlServerStatement.setString(66, resultSet.getString("CMK_S_FIELD_CODE_QUALIFICATION_MERE"));
                sqlServerStatement.setString(67, resultSet.getString("CMK_S_FIELD_NUM_QUALIFICATION"));
                sqlServerStatement.setString(68, resultSet.getString("CMK_S_FIELD_QUALIF_TEL1"));
                sqlServerStatement.setString(69, resultSet.getString("CMK_S_FIELD_QUALIF_TEL2"));
                sqlServerStatement.setString(70, resultSet.getString("CMK_S_FIELD_QUALIF_TEL3"));
                sqlServerStatement.setString(71, resultSet.getString("CMK_S_FIELD_QUALIF_FAX1"));
                sqlServerStatement.setString(72, resultSet.getString("CMK_S_FIELD_QUALIF_FAX2"));
                sqlServerStatement.setString(73, resultSet.getString("CMK_S_FIELD_DATE_QUALIF_TEL1"));
                sqlServerStatement.setString(74, resultSet.getString("CMK_S_FIELD_DATE_QUALIF_TEL2"));
                sqlServerStatement.setString(75, resultSet.getString("CMK_S_FIELD_DATE_QUALIF_TEL3"));
                sqlServerStatement.setString(76, resultSet.getString("CMK_S_FIELD_DATE_QUALIF_FAX1"));
                sqlServerStatement.setString(77, resultSet.getString("CMK_S_FIELD_DATE_QUALIF_FAX2"));
                sqlServerStatement.setString(78, resultSet.getString("CMK_S_FIELD_DATEHEURETRAITEMENT"));
                sqlServerStatement.setString(79, resultSet.getString("CMK_S_FIELD_DATETRAITEMENT"));
                sqlServerStatement.setString(80, resultSet.getString("CMK_S_FIELD_HEURETRAITEMENT"));
                sqlServerStatement.setString(81, resultSet.getString("CMK_S_FIELD_NOMCAMPAGNE"));
                sqlServerStatement.setString(82, resultSet.getString("CMK_S_FIELD_NOMFICHIER"));
                sqlServerStatement.setString(83, resultSet.getString("CMK_S_FIELD_COMMENTAIRES"));
                sqlServerStatement.setString(84, resultSet.getString("CMK_S_FIELD_NUM_IMPORT"));
                sqlServerStatement.setString(85, resultSet.getString("CMK_S_FIELD_AGENT_NOM"));
                sqlServerStatement.setString(86, resultSet.getString("CMK_S_FIELD_AGENT_LOGIN"));
                sqlServerStatement.setString(87, resultSet.getString("CMK_S_FIELD_COMMERCIAL_NOM"));
                sqlServerStatement.setString(88, resultSet.getString("nums_observation_contact"));
                sqlServerStatement.executeUpdate();
            }

            
            resultSet.close();
            mysqlStatement.close();
            sqlServerStatement.close();
            mysqlConnection.close();
            sqlServerConnection.close();

            System.out.println("Transfert des données terminé avec succès.");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
