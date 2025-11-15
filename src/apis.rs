#![allow(dead_code)]

use serde_json::{Value, json};

#[derive(Debug, Clone)]
pub enum Ep {
  Jsons,
  Sheets,  // w 300, r 60
  Secrets, // w 2400, r 60,000
  Admins,
  Orgbase,
  Lictyp,
  Roles,           // 2400
  RoleAssignments, // 2400
  Orgs,            // 2400
  Groups,          // 2400
  Members,         // 2400
  Users,           // 2400
  Lics,            // 120
  Courses,         // 6000, 3000 per user!
  Topics,          // 6000, 3000 per user!
  Teachers,        // 6000, 3000 per user!
  Students,        // 6000, 3000 per user!
  Guardians,       // 6000 - handles both guardians and invitations
  CourseWork,      // 6000, 3000 per user!
  Grades,          // 6000, 3000 per user!
  Gradsav,
  Gradspon,
  Chromebooks, // 60
  Drives,
  Files,
  Calendars,
  Events,
  Spaces, // 6000, cr space 60, members 300
  UsageReports,
  Emails,
}

impl Ep {
  pub fn base_url(&self) -> &'static str {
    match self {
      Ep::Spaces => "https://chat.googleapis.com/v1/",
      Ep::Secrets => "https://secretmanager.googleapis.com/v1/projects/",
      Ep::Jsons | Ep::Admins | Ep::Orgbase | Ep::Lictyp | Ep::Sheets => {
        "https://sheets.googleapis.com/v4/spreadsheets/"
      }
      Ep::Roles => {
        "https://admin.googleapis.com/admin/directory/v1/customer/my_customer/roles/"
      }
      Ep::RoleAssignments => {
        "https://admin.googleapis.com/admin/directory/v1/customer/my_customer/roleAssignments/"
      }
      Ep::Orgs => {
        "https://admin.googleapis.com/admin/directory/v1/customer/my_customer/orgunits/"
      }
      Ep::Groups => "https://admin.googleapis.com/admin/directory/v1/groups/",
      Ep::Members => "https://admin.googleapis.com/admin/directory/v1/groups/",
      Ep::Users => "https://admin.googleapis.com/admin/directory/v1/users/",
      Ep::Lics => "https://licensing.googleapis.com/apps/licensing/v1/product/",
      Ep::Courses
      | Ep::Topics
      | Ep::CourseWork
      | Ep::Grades
      | Ep::Gradsav
      | Ep::Gradspon
      | Ep::Teachers
      | Ep::Students => "https://classroom.googleapis.com/v1/courses/",
      Ep::Guardians => "https://classroom.googleapis.com/v1/userProfiles/",
      Ep::Chromebooks => {
        "https://admin.googleapis.com/admin/directory/v1/customer/my_customer/devices/chromeos/"
      }
      Ep::Files => "https://www.googleapis.com/drive/v3/files/",
      Ep::Drives => "https://www.googleapis.com/drive/v3/drives/",
      Ep::Calendars | Ep::Events => {
        "https://www.googleapis.com/calendar/v3/calendars/"
      }
      Ep::UsageReports => {
        "https://admin.googleapis.com/admin/reports/v1/usage/users/all/dates/"
      }
      Ep::Emails => "https://gmail.googleapis.com/gmail/v1/users/me/messages/",
    }
  }

  // Helper method to get res_obs for guardians based on invitation status
  pub fn res_obs_guar(&self, is_invitation: bool) -> &'static str {
    match self {
      Ep::Guardians => {
        if is_invitation {
          "guardianInvitations"
        } else {
          "guardians"
        }
      }
      _ => self.res_obs(),
    }
  }

  pub fn res_obs(&self) -> &'static str {
    match self {
      Ep::Secrets => "secrets",
      Ep::Jsons | Ep::Admins | Ep::Orgbase | Ep::Lictyp | Ep::Sheets => {
        "values"
      }
      Ep::Roles
      | Ep::RoleAssignments
      | Ep::Lics
      | Ep::Calendars
      | Ep::Events => "items",
      Ep::Orgs => "organizationUnits",
      Ep::Groups => "groups",
      Ep::Members => "members",
      Ep::Users => "users",
      Ep::Chromebooks => "chromeosdevices",
      Ep::Courses => "courses",
      Ep::Topics => "topic",
      Ep::Teachers => "teachers",
      Ep::Students => "students",
      Ep::Guardians => "guardians", // Default, will be overridden when needed
      Ep::CourseWork => "courseWork",
      Ep::Grades | Ep::Gradsav | Ep::Gradspon => "studentSubmissions",
      Ep::Drives => "drives",
      Ep::Files => "files",
      Ep::Spaces => "spaces",
      Ep::UsageReports => "usageReports",
      Ep::Emails => "messages",
    }
  }

  pub fn table_sheet(&self) -> &'static str {
    match self {
      Ep::Jsons => "jsons",
      Ep::Sheets => "values",
      Ep::Secrets => "secrets",
      Ep::Admins => "alladmins",
      Ep::Orgbase => "orgbase",
      Ep::Lictyp => "lictyp",
      Ep::Roles => "roles",
      Ep::RoleAssignments => "rassigned",
      Ep::Orgs => "orgs",
      Ep::Groups => "grupos",
      Ep::Members => "miembros",
      Ep::Users => "usuarios",
      Ep::Lics => "licencias",
      Ep::Courses => "cursos",
      Ep::Topics => "temas",
      Ep::Teachers => "profesores",
      Ep::Students => "alumnos",
      Ep::Guardians => "tutores",
      Ep::CourseWork => "tareas",
      Ep::Grades => "calificaciones",
      Ep::Gradsav => "gradsav",
      Ep::Gradspon => "gradspon",
      Ep::Chromebooks => "chromebooks",
      Ep::Drives => "unidades",
      Ep::Files => "archivos",
      Ep::Calendars => "calendarios",
      Ep::Events => "eventos",
      Ep::Spaces => "espacios",
      Ep::UsageReports => "reportes",
      Ep::Emails => "mensajes",
    }
  }

  pub fn google_table(&self) -> &'static str {
    match self {
      Ep::Jsons => "gjsons",
      Ep::Sheets => "gvalues",
      Ep::Secrets => "gsecrets",
      Ep::Admins => "galladmins",
      Ep::Orgbase => "gorgbase",
      Ep::Lictyp => "glictyp",
      Ep::Roles => "groles",
      Ep::RoleAssignments => "grassigned",
      Ep::Orgs => "gorgs",
      Ep::Groups => "ggrupos",
      Ep::Members => "gmiembros",
      Ep::Users => "gusuarios",
      Ep::Lics => "glicencias",
      Ep::Courses => "gcursos",
      Ep::Topics => "gtemas",
      Ep::Teachers => "gprofesores",
      Ep::Students => "galumnos",
      Ep::Guardians => "gtutores",
      Ep::CourseWork => "gtareas",
      Ep::Grades => "gcalificaciones",
      Ep::Gradsav => "ggradsav",
      Ep::Gradspon => "ggradspon",
      Ep::Chromebooks => "gchromebooks",
      Ep::Drives => "gunidades",
      Ep::Files => "garchivos",
      Ep::Calendars => "gcalendarios",
      Ep::Events => "geventos",
      Ep::Spaces => "gespacios",
      Ep::UsageReports => "greportes",
      Ep::Emails => "mensajes",
    }
  }

  // the second bool argument her is only for the tutors enum problem
  pub fn strtoenum(sheet: &str, _is_invit: bool) -> Option<Self> {
    match sheet {
      "jsons" => Some(Ep::Jsons),
      "values" => Some(Ep::Sheets),
      "secrets" => Some(Ep::Secrets),
      "alladmins" => Some(Ep::Admins),
      "orgbase" => Some(Ep::Orgbase),
      "roles" => Some(Ep::Roles),
      "rassigned" => Some(Ep::RoleAssignments),
      "orgs" => Some(Ep::Orgs),
      "grupos" => Some(Ep::Groups),
      "miembros" => Some(Ep::Members),
      "usuarios" => Some(Ep::Users),
      "licencias" => Some(Ep::Lics),
      "cursos" => Some(Ep::Courses),
      "temas" => Some(Ep::Topics),
      "profesores" => Some(Ep::Teachers),
      "alumnos" => Some(Ep::Students),
      "tutores" => Some(Ep::Guardians),
      "tareas" => Some(Ep::CourseWork),
      "calificaciones" => Some(Ep::Grades),
      "gradsav" => Some(Ep::Gradsav),
      "gradspon" => Some(Ep::Gradspon),
      "chromebooks" => Some(Ep::Chromebooks),
      "archivos" => Some(Ep::Files),
      "uniidades" => Some(Ep::Drives),
      "calendarios" => Some(Ep::Calendars),
      "eventos" => Some(Ep::Events),
      "espacios" => Some(Ep::Spaces),
      "reportes" => Some(Ep::UsageReports),
      "mensajes" => Some(Ep::Emails),
      _ => None, // Return `None` if no match is found
    }
  }

  pub fn enumcmd(cmd: &str) -> Self {
    match cmd {
      "jsons" => Ep::Jsons,
      "values" => Ep::Sheets,
      "secrets" => Ep::Secrets,
      "alladmins" => Ep::Admins,
      "orgbase" => Ep::Orgbase,
      "roles" => Ep::Roles,
      "rassigned" => Ep::RoleAssignments,
      "ldr" => Ep::Drives,
      "lo" | "dor" | "co" | "cobo" | "uo" => Ep::Orgs,
      "lg" | "dg" | "cg" | "cobg" | "cobcg" | "aobg" | "ccg" | "ccgex"
      | "ug" => Ep::Groups,
      "lm" | "lma" | "amg" | "rmg" | "umg" => Ep::Members,
      "lud" | "lu" | "cu" | "cup" | "cud" | "cuf" | "uu" | "uuo" | "du" | "duni" | "udu"
      | "su" | "usu" | "au" | "uau" | "cp" | "cpf" | "cpfc" | "cps" | "cpfs" | "cpc"
      | "suc" | "usuc" | "cog" | "cogc" | "cgc" | "coga" | "cogal"
      | "cogalc" | "cogsu" | "cogusu" | "cogsua" | "cogusua" | "ag" | "rg"
      | "afo" | "dfo" | "asc" | "dua" | "duaa" | "aua" | "auaa" | "gfe"
      | "giu" | "iden" | "ides" | "idfr" | "idge" | "idit" | "csc" | "dsc" => {
        Ep::Users
      }
      "li" | "ali" | "uli" | "qli" => Ep::Lics,
      "lc" | "oc" | "uc" | "cc" | "ac" | "rac" | "adsc" | "dac" | "apc"
      | "dpc" | "ascl" | "rscl" | "apcl" | "rpcl" | "racl" | "dc" => {
        Ep::Courses
      }
      "lta" | "lt" => Ep::Topics,
      "lp" | "lpa" => Ep::Teachers,
      "ls" | "lsa" => Ep::Students,
      "ltu" | "atu" | "dtu" | "dtui" => Ep::Guardians,
      "la" | "ct" | "mt" | "dt" => Ep::CourseWork,
      "lgr" | "lgra" | "mgr" => Ep::Grades,
      "lgrav" => Ep::Gradsav,
      "lgrap" => Ep::Gradspon,
      "lcb" | "upcb" | "scb" | "uscb" | "rcb" | "fcb" => Ep::Chromebooks,
      "lv" | "ldu" | "ldd" | "cod" | "dff" | "ddm" => Ep::Files,
      "lca" | "cca" | "mca" | "dca" => Ep::Calendars,
      "le" | "mev" | "cev" | "dev" => Ep::Events,
      "lsp" => Ep::Spaces,
      "lre" | "lreu" => Ep::UsageReports,
      "dfe" => Ep::Emails,
      _ => Ep::Sheets,
    }
  }

  pub fn chstli(&self) -> String {
    let item = match self {
      Ep::Jsons => "jsons",
      Ep::Sheets => "values",
      Ep::Secrets => "secrets",
      Ep::Admins => "alladmins",
      Ep::Orgbase => "orgbase",
      Ep::Lictyp => "lictyp",
      Ep::Roles => "roles",
      Ep::RoleAssignments => "rassigned",
      Ep::Orgs => "orgs",
      Ep::Groups => "grupos",
      Ep::Members => "miembros",
      Ep::Users => "usuarios",
      Ep::Lics => "licencias",
      Ep::Courses => "cursos",
      Ep::Topics => "temas",
      Ep::Teachers => "profesores",
      Ep::Students => "alumnos",
      Ep::Guardians => "tutores",
      Ep::CourseWork => "tareas",
      Ep::Grades => "calificaciones",
      Ep::Gradsav => "gradsav",
      Ep::Gradspon => "gradspon",
      Ep::Chromebooks => "chromebooks",
      Ep::Drives => "unidades",
      Ep::Files => "archivos",
      Ep::Calendars => "calendarios",
      Ep::Events => "eventos",
      Ep::Spaces => "espacios",
      Ep::UsageReports => "reportes",
      Ep::Emails => "mensajes",
    };
    format!(
      "Con gusto saco la lista de {item}. Deme unos segundos o minutos para hacerlo (dependiendo de cuantas {item} son)."
    )
  }

  pub fn chendli(&self, num: i64, tims: String) -> String {
    let item = match self {
      Ep::Jsons => "jsons",
      Ep::Sheets => "values",
      Ep::Secrets => "secrets",
      Ep::Admins => "alladmins",
      Ep::Orgbase => "orgbase",
      Ep::Lictyp => "lictyp",
      Ep::Roles => "roles",
      Ep::RoleAssignments => "rassigned",
      Ep::Orgs => "orgs",
      Ep::Groups => "grupos",
      Ep::Members => "miembros",
      Ep::Users => "usuarios",
      Ep::Lics => "licencias",
      Ep::Courses => "cursos",
      Ep::Topics => "temas",
      Ep::Teachers => "profesores",
      Ep::Students => "alumnos",
      Ep::Guardians => "tutores",
      Ep::CourseWork => "tareas",
      Ep::Grades => "calificaciones",
      Ep::Gradsav => "gradsav",
      Ep::Gradspon => "gradspon",
      Ep::Chromebooks => "chromebooks",
      Ep::Drives => "unidades",
      Ep::Files => "archivos",
      Ep::Calendars => "calendarios",
      Ep::Events => "eventos",
      Ep::Spaces => "espacios",
      Ep::UsageReports => "reportes",
      Ep::Emails => "mensajes",
    };
    format!("Hay {num} {item} en el dominio. El proceso tomó {tims}.")
  }

  pub fn chstmod(&self, comdo: String) -> String {
    let item = match self {
      Ep::Jsons => "jsons",
      Ep::Sheets => "values",
      Ep::Secrets => "secrets",
      Ep::Admins => "alladmins",
      Ep::Orgbase => "orgbase",
      Ep::Lictyp => "lictyp",
      Ep::Roles => "roles",
      Ep::RoleAssignments => "rassigned",
      Ep::Orgs => "orgs",
      Ep::Groups => "grupos",
      Ep::Members => "miembros",
      Ep::Users => "usuarios",
      Ep::Lics => "licencias",
      Ep::Courses => "cursos",
      Ep::Topics => "temas",
      Ep::Teachers => "profesores",
      Ep::Students => "alumnos",
      Ep::Guardians => "tutores",
      Ep::CourseWork => "tareas",
      Ep::Grades => "calificaciones",
      Ep::Gradsav => "gradsav",
      Ep::Gradspon => "gradspon",
      Ep::Chromebooks => "chromebooks",
      Ep::Drives => "unidades",
      Ep::Files => "archivos",
      Ep::Calendars => "calendarios",
      Ep::Events => "eventos",
      Ep::Spaces => "espacios",
      Ep::UsageReports => "reportes",
      Ep::Emails => "mensajes",
    };
    format!(
      "Con gusto inicio el proceso de modificar l@s {item} con respecto al comando {comdo}. Deme unos segundos o minutos para hacerlo, dependiendo de cuant@s {item} son."
    )
  }

  pub fn chendmod(&self, comdo: String, tims: String, errs: String) -> String {
    let item = match self {
      Ep::Jsons => "jsons",
      Ep::Sheets => "values",
      Ep::Secrets => "secrets",
      Ep::Admins => "alladmins",
      Ep::Orgbase => "orgbase",
      Ep::Lictyp => "lictyp",
      Ep::Roles => "roles",
      Ep::RoleAssignments => "rassigned",
      Ep::Orgs => "orgs",
      Ep::Groups => "grupos",
      Ep::Members => "miembros",
      Ep::Users => "usuarios",
      Ep::Lics => "licencias",
      Ep::Courses => "cursos",
      Ep::Topics => "temas",
      Ep::Teachers => "profesores",
      Ep::Students => "alumnos",
      Ep::Guardians => "tutores",
      Ep::CourseWork => "tareas",
      Ep::Grades => "calificaciones",
      Ep::Gradsav => "gradsav",
      Ep::Gradspon => "gradspon",
      Ep::Chromebooks => "chromebooks",
      Ep::Drives => "unidades",
      Ep::Files => "archivos",
      Ep::Calendars => "calendarios",
      Ep::Events => "eventos",
      Ep::Spaces => "espacios",
      Ep::UsageReports => "reportes",
      Ep::Emails => "mensajes",
    };
    format!(
      "Ya se terminó los procesos de modificar l@s {item} con respecto al comando {comdo}. {errs} El proceso tomó {tims}."
    )
  }

  pub fn modvel(&self) -> usize {
    match self {
      Ep::Jsons => 10,
      Ep::Sheets => 1,
      Ep::Secrets => 35,
      Ep::Admins => 7,
      Ep::Orgbase => 7,
      Ep::Lictyp => 2,
      Ep::Roles => 7,
      Ep::RoleAssignments => 7,
      Ep::Orgs => 1,
      Ep::Groups => 20,
      Ep::Members => 35,
      Ep::Users => 20,
      Ep::Lics => 2,
      Ep::Courses => 7,
      Ep::Topics => 7,
      Ep::CourseWork => 7,
      Ep::Grades => 7,
      Ep::Gradsav => 45,
      Ep::Gradspon => 45,
      Ep::Teachers => 7,
      Ep::Students => 7,
      Ep::Guardians => 7,
      Ep::Chromebooks => 1,
      Ep::Drives => 10,
      Ep::Files => 90,
      Ep::Calendars => 7,
      Ep::Events => 7,
      Ep::Spaces => 7,
      Ep::UsageReports => 7,
      Ep::Emails => 7,
    }
  }

  pub fn modvlow(&self) -> usize {
    match self {
      Ep::Jsons => 10,
      Ep::Sheets => 1,
      Ep::Secrets => 35,
      Ep::Admins => 1,
      Ep::Orgbase => 1,
      Ep::Lictyp => 2,
      Ep::Roles => 10,
      Ep::RoleAssignments => 10,
      Ep::Orgs => 1,
      Ep::Groups => 5,
      Ep::Members => 2,
      Ep::Users => 8,
      Ep::Lics => 1,
      Ep::Courses => 7,
      Ep::Topics => 3,
      Ep::Teachers => 3,
      Ep::Students => 3,
      Ep::Guardians => 10,
      Ep::CourseWork => 3,
      Ep::Grades => 3,
      Ep::Gradsav => 45,
      Ep::Gradspon => 45,
      Ep::Chromebooks => 1,
      Ep::Drives => 10,
      Ep::Files => 90,
      Ep::Calendars => 10,
      Ep::Events => 10,
      Ep::Spaces => 90,
      Ep::UsageReports => 10,
      Ep::Emails => 10,
    }
  }

  pub fn modlst(&self) -> usize {
    match self {
      Ep::Jsons => 10,
      Ep::Sheets => 1,
      Ep::Secrets => 35,
      Ep::Admins => 10,
      Ep::Orgbase => 10,
      Ep::Lictyp => 2,
      Ep::Roles => 10,
      Ep::RoleAssignments => 10,
      Ep::Orgs => 1,
      Ep::Groups => 22,
      Ep::Members => 22,
      Ep::Users => 100,
      Ep::Lics => 2,
      Ep::Courses => 40,
      Ep::Topics => 40,
      Ep::Teachers => 40,
      Ep::Students => 40,
      Ep::Guardians => 10,
      Ep::CourseWork => 40,
      Ep::Grades => 40,
      Ep::Gradsav => 45,
      Ep::Gradspon => 45,
      Ep::Chromebooks => 1,
      Ep::Drives => 10,
      Ep::Files => 90,
      Ep::Calendars => 10,
      Ep::Events => 10,
      Ep::Spaces => 10,
      Ep::UsageReports => 10,
      Ep::Emails => 10,
    }
  }

  // FIX we need another query function to update the rows so as not to hav to call the lists function
  // Helper method to get guardian queries based on invitation status
  pub fn qrys_guar(&self, _is_invitation: bool) -> String {
    match self {
      Ep::Guardians => {
        // Unified query that handles both guardian types in one query
        let item = "
        'x' as do,
        if data.invitationId is not none then
          data.invitationId
        else
          data.guardianId
        end as id,
        data.invitedEmailAddress as correo_tutor,
        (select value data.correo from usuarios
          where data.id = $parent.data.studentId
          and abr = $abr
        )[0] as correo_alumno,
        if data.invitationId is not none then
          data.state
        else
          'COMPLETE'
        end as estado,
        (select value data.grupo from usuarios
          where data.id = $parent.data.studentId
          and abr = $abr
        )[0] as grupo
        ";

        let query_template_str = r#"
          select
            {} 
          from type::table($gtable) 
          where abr = $abr
        "#;

        query_template_str.replace("{}", item)
      }
      _ => self.qrys(),
    }
  }

  pub fn qrys(&self) -> String {
    let item = match self {
      Ep::Jsons => "
        'x' as do,
        "
      .to_string(),
      Ep::Sheets => "
        'x' as do,
        "
      .to_string(),
      Ep::Emails => "
        'x' as do,
        "
      .to_string(),
      Ep::Secrets => "
        'x' as do,
        data. name as nombre,
        data.createTime as tiempo_creacion
        "
      .to_string(),
      Ep::Admins => "
        'x' as do,
        data.abr as abr,
        data.dominio as dominio,
        data.jsonf as jsonf,
        data.correo as correo,
        data.spreadsheet_id as spreadsheet_id,
        data.admins as admins,
        data.bigqid as bigqid       
        "
      .to_string(),
      Ep::Orgbase => "
        'x' as do,
        data.id as id,
        data.nombre as nombre,
        data.org_principal as org_principal,
        data.grupo_principal as grupo_principal,
        data.org as org,
        data.grupo as grupo
        "
      .to_string(),
      Ep::Lictyp => "
        'x' as do,
        data.nombre_producto as nombre_producto,
        data.pid as pid,
        data.nombre_sku as nombre_sku,
        data.skuid as skuid,
        data.abrlic as abrlic
        "
      .to_string(),
      Ep::Roles => "
        'x' as do,
        data.roleId as id,
        data.roleName as nombre,
        data.roleDescription as descripcion,
        data.isSuperAdminRole as es_superadmin,
        data.isSystemRole as es_sistema,
        array::join(data.rolePrivileges.*.serviceId, ', ') as ids_servicio,
        array::join(data.rolePrivileges.*.privilegeName, ', ') as nombres_priv,
        data.kind as tipo
        "
      .to_string(),
      //  data.someArray[0].property
      // array::join(
      //   array::map(data.rolePrivileges, |$item| {
      //     string::concat($item.serviceId, ':', $item.privilegeName)
      //   }),
      //   ', '
      // ) as service_privilege_pairs
      Ep::RoleAssignments => "
        'x' as do,
        data.roleAssignmentId as id,
        data.roleId as id_roles,
        data.assignedTo as asignado,
        data.assigneeType as tipo,
        data.scopeType as tipo_scopo,
        if string::starts_with(data.orgUnitId, 'id:') then string::slice(data.orgUnitId, 3) else data.orgUnitId end as id_org,
        data.condition as condicion,        
        data.kind as tipo
        "
      .to_string(),

      // string::slice(data.orgUnitId, 3) as id_org
    
      Ep::Orgs => "
        'x' as do,
        if string::starts_with(data.orgUnitId, 'id:') then string::slice(data.orgUnitId, 3) else data.orgUnitId end as id,
        data.name as nombre,
        data.description as descripcion,
        data.orgUnitPath as ruta,
        data.parentOrgUnitPath as ruta_principal
        "
      .to_string(),
      Ep::Groups => "
        'x' as do,
        data.id as id,
        data.name as nombre,
        data.description as descripcion,
        data.email as correo,
        string::split(data.email, '@')[0] as abrv
        "
      .to_string(),
      // Fixed: grupo comes from the group_email field added during google_to_gdatabase
      Ep::Members => "
        'x' as do,
        data.id as id,
        extra.key1 as grupo,
        '' as modificar_grupo,
        data.email as correo,
        data.kind as clase,
        data.role as rol,
        data.type as tipo,
        data.status as estatus
        "
      .to_string(),
      Ep::Users => "
        'x' as do,
        data.id as id,
        data.primaryEmail as correo,
        data.name.givenName as nombres,
        data.name.familyName as apellidos,
        data.password as contrasena,
        data.changePasswordAtNextLogin as cambiar_pass,
        (select value data.grupo from orgbase
          where data.org = $parent.data.orgUnitPath
          and abr = $abr
        )[0] as grupo,
        '' as modificar_grupo,
        data.orgUnitPath as org,
        data.suspended as suspendido,
        data.archived as archivado,
        data.organizations[0].costCenter as cost_center,
        data.organizations[0].department as departamento,
        data.isAdmin as es_admin,
        data.isDelegatedAdmin as es_sub_admin,
        data.isEnrolledIn2Sv as sv2_activo,
        data.isEnforcedIn2Sv as sv2_obligado,
        data.includeInGlobalAddressList as lista_global,
        data.creationTime as fecha_creada,
        data.lastLoginTime as login_ultimo,
        data.deletionTime as eliminado,
        data.languages[0].languageCode as idioma,
        data.thumbnailPhotoUrl as foto_perfil,
        data.recoveryPhone as numero_cell,
        array::join(data.aliases ?? [], ', ') as aliases, 
        '' as correos_reenvio
        "
      .to_string(),
      Ep::Lics => "
        'x' as do,
        rand::guid(10) as id,
        data.userId as correo_usuario,
        data.productName as nombre_producto,
        data.productId as pid,
        data.skuName as nombre_sku,
        data.skuId as skuid,
        (select value data.abrlic from lictyp
          where data.skuid = $parent.data.skuId
        )[0] as abrlic,
        (select value data.grupo from usuarios
          where data.correo = $parent.data.userId
          and abr = $abr
        )[0] as grupo
        "
      .to_string(),
      Ep::Courses => "
        'x' as do,
        data.id as id,
        data.name as nombre,
        data.section as seccion, 
        data.descriptionHeading as desc_cabeza,
        data.description as descripcion,
        data.room as sala,

        (select value data.correo from usuarios
          where data.id = $parent.data.ownerId
          and abr = $abr
        )[0] ?? 'ELIMINADO' as correo_dueno,
        
        '' as correo_dos,
        '' as grupo_dos,
        
        data.creationTime as fecha_creada,
        data.updateTime as fecha_actualizada,
        data.enrollmentCode as codigo_enrolar,
        data.courseState as estado,
        data.guardiansEnabled as activado_tutores,
        if data.gradebookSettings.gradeCategories is not none then 
          data.gradebookSettings.gradeCategories
            .filter(
              |$category| $category.name is not none
              and $category.weight is not none
            )
            .map(
              |$category| string::concat(
                $category.name, ':', <string>($category.weight / 10000)
              )
            )
            .join(', ')
        else 
          ''
        end as categorias  
        "
      .to_string(),

      // NOTE this goes under mods
      Ep::Topics => "
        'x' as do,
        data.topicId as id,
        data.courseId as id_curso,
        data.name as nombre_tema,
        (select value data.nombre from cursos
          where data.id = $parent.data.courseId
          and abr = $abr
        )[0] as nombre_curso,
        data.updateTime as fecha_actualizada
        "
      .to_string(),
      // NOTE this goes under mods
      Ep::Teachers => "
        'x' as do,
        rand::guid(10) as id,
        data.courseId as id_curso,
        (select value data.nombre from cursos
          where data.id = $parent.data.courseId
          and abr = $abr
        )[0] as nombre_curso,
        (select value data.correo from usuarios
          where data.id = $parent.data.userId
          and abr = $abr
        )[0] as correo_dueno
        "
      .to_string(),
      // NOTE this goes under mods
      Ep::Students => "
        'x' as do,
        rand::guid(10) as id,
        data.courseId as id_curso,
        (select value data.nombre from cursos
          where data.id = $parent.data.courseId
          and abr = $abr
        )[0] as nombre_curso,
        (select value data.correo from usuarios
          where data.id = $parent.data.userId
          and abr = $abr
        )[0] as correo_alumno
        "
      .to_string(),
      // NOTE this goes under mods
      Ep::Guardians => "
        'x' as do,
        data.guardianId as id,
        data.invitedEmailAddress as correo_tutor,
        (select value data.correo from usuarios
          where data.id = $parent.data.studentId
          and abr = $abr
        )[0] as correo_alumno,
        'COMPLETE' as estado,
        (select value data.grupo from usuarios
          where data.id = $parent.data.studentId
          and abr = $abr
        )[0] as grupo
        "
      .to_string(),
      // NOTE this goes under mods
      Ep::CourseWork => "
        'x' as do,
        data.id as id,
        data.courseId as id_curso,
        (select value data.nombre from cursos
          where data.id = $parent.data.courseId
        )[0] as nombre_curso,
        (select value data.correo from usuarios
          where data.id = $parent.data.creatorUserId
          and abr = $abr
        )[0] as correo_dueno,
        data.title as titulo,
        data.description as descripcion,
        data.state as estado,
        data.creationTime as fecha_creada,
        data.updateTime as fecha_actualizada,
        data.scheduledTime as fecha_programada,        
        if data.dueDate.year is not none then
          string::concat(
            data.dueDate.year, '-',
            string::slice(string::concat('0', data.dueDate.month), -2), '-',
            string::slice(string::concat('0', data.dueDate.day), -2), 'T',
            string::slice(string::concat('0', if data.dueTime.hours is not none then data.dueTime.hours else 0 end), -2), ':',
            string::slice(string::concat('0', if data.dueTime.minutes is not none then data.dueTime.minutes else 0 end), -2), ':',
            string::slice(string::concat('0', if data.dueTime.seconds is not none then data.dueTime.seconds else 0 end), -2)
          )
        else
          ''
        end as fecha_vencida,
        data.maxPoints as puntos,
        data.workType as tipo_trabajo,
        data.associatedWithDeveloper as bot_hecho,
        data.topicId as id_tema,
        data.assigneeMode as modo,

        data.gradeCategory.name as nombre_categoria,
        if data.gradeCategory.weight is not none then
          math::round(data.gradeCategory.weight / 10000)
        else
          ''
        end as peso_categoria,        
        data.gradingPeriodId as id_periodo,
        data.assignment.studentWorkFolder.id as id_carpeta_alumno,
        data.submissionModificationMode as modificable,
        data.alternateLink as enlace,
        if data.multipleChoiceQuestion is not none then
          array::join(data.multipleChoiceQuestion.choices, ', ')
        else
          ''
        end as opciones,
        if data.materials is not none then
          data.materials
            .filter(|$material| $material.form is not none)
            .map(|$material| $material.form.formUrl)
            .join(', ')
        else
          ''
        end as links_formulario,
        if data.materials is not none then
          data.materials
            .filter(|$material| $material.link is not none)
            .map(|$material| $material.link.url)
            .join(', ')
        else
          ''
        end as links_url,
        if data.materials is not none then
          data.materials
            .filter(|$material| $material.youtubeVideo is not none)
            .map(|$material| $material.youtubeVideo.alternateLink)
            .join(', ')
        else
          ''
        end as links_youtube,
        if data.materials is not none then
          data.materials
            .filter(|$material| $material.driveFile is not none)
            .map(|$material| $material.driveFile.driveFile.alternateLink)
            .join(', ')
        else
          ''
        end as links_drive
        "
      .to_string(),
      // NOTE this goes under mods
      Ep::Grades => "
        'x' as do,
        data.id as id,
        data.courseId as id_curso,
        (select value data.nombre from cursos
          where data.id = $parent.data.courseId
          and abr = $abr
        )[0] as nombre_curso,
        (select value data.correo_dueno from cursos
          where data.id = $parent.data.courseId
          and abr = $abr
        )[0] as correo_dueno, data.courseWorkId as id_tarea,
        (select value data.titulo from tareas
          where data.id = $parent.data.courseWorkId
          and abr = $abr
        )[0] as nombre_tarea,
        (select value data.correo from usuarios
          where data.id = $parent.data.userId
          and abr = $abr
        )[0] as correo_alumno,
        data.assignedGrade ?? 0 as calificacion,
        (select value data.nombre_categoria from tareas
          where data.id = $parent.data.courseWorkId
          and abr = $abr
          and data.nombre_categoria != null
        )[0] as nombre_categoria,
        data.late as tarde,
        data.courseWorkType as tipo_tarea,
        data.state as estado,
        data.creationTime as fecha_creada,
        data.updateTime as fecha_actualizada,
        data.associatedWithDeveloper as bot_hecho,
        data.rubricId as id_rubrica,
        data.assignedRubricGrades.points as calif_rubrica,
        data.shortAnswerSubmission.answer as respusta_breve, 
        data.multipleChoiceSubmission.answer as respuesta_mul
        "
      .to_string(),    
      Ep::Gradspon => "
        let $users = (
          select 
            data.correo as correo_alumno, 
            data.departamento as matricula, 
            data.grupo as grupo, 
            string::replace(
              string::concat(
                string::uppercase(
                  string::replace(data.apellidos, <regex>'[\\n\\r\\f]+', '')
                ), 
                ' ', 
                string::uppercase(
                  string::replace(data.nombres, <regex>'[\\n\\r\\f]+', '')
                )
              ), 
              '  ', 
              ' '
            ) as nombre_alumno 
          from usuarios 
          where abr = $abr
        ); 

        let $cursos = (
          select 
            data.id as id_curso, 
            data.nombre as nombre_curso, 
            data.seccion as seccion, 
            data.correo_dueno as correo_dueno, 
            data.desc_cabeza as id_servo 
          from cursos 
          where abr = $abr
        ); 

        let $grades = (
          select 
            math::mean(data.calificacion) as average, 
            data.correo_alumno, 
            data.id_curso, 
            data.nombre_categoria 
          from calificaciones 
          where string::len(data.nombre_categoria) > 0 
            and data.calificacion is not none 
            and abr = $abr 
          group by data.correo_alumno, data.id_curso, data.nombre_categoria
        ); 

        let $final_results = (
          select 
            'x' as do,
            rand::guid(10) as id,
            array::filter($users, |$user| $user.correo_alumno = data.correo_alumno)[0].nombre_alumno as nombre_alumno,
            data.correo_alumno as correo_alumno,
            array::filter($users, |$user| $user.correo_alumno = data.correo_alumno)[0].matricula as matricula,
            data.id_curso as id_curso,
            array::filter($cursos, |$curso| $curso.id_curso = data.id_curso)[0].nombre_curso as nombre_curso,
            array::filter($cursos, |$curso| $curso.id_curso = data.id_curso)[0].seccion as seccion,
            array::filter($cursos, |$curso| $curso.id_curso = data.id_curso)[0].correo_dueno as correo_dueno,
            array::filter($cursos, |$curso| $curso.id_curso = data.id_curso)[0].id_servo as id_servo,
            array::filter($users, |$user| $user.correo_alumno = data.correo_alumno)[0].grupo as grupo,
            data.nombre_categoria as nombre_categoria,
            <number>math::round(average * 100) / 100 as promedio_cat
          from $grades 
          order by data.correo_alumno, data.nombre_categoria
        );
        "
      .to_string(),
      // NOTE for gradsav, you need to CHANGE the if data.peso_categoria = '' then 40  part later on, it should be 0 or 100
      Ep::Gradsav => "
        let $users = (
          select
            data.correo as correo_alumno,
            data.departamento as matricula,
            data.grupo as grupo,
            string::replace(
              string::concat(
                string::uppercase(
                  string::replace(data.apellidos, <regex>'[\\n\\r\\f]+', '')
                ),
                ' ',
                string::uppercase(
                  string::replace(data.nombres, <regex>'[\\n\\r\\f]+', '')
                )
              ),
              '  ',
              ' '
            ) as nombre_alumno
          from usuarios
          where abr = $abr
        );

        let $cursos = (
          select
            data.id as id_curso,
            data.nombre as nombre_curso,
            data.seccion as seccion,
            data.correo_dueno as correo_dueno,
            data.desc_cabeza as id_servo
          from cursos
          where abr = $abr
        );

        let $tareas = (
          select
            data.id as id_tarea,
            data.nombre_categoria as categoria,
            if data.peso_categoria = '' then 40 else <int>data.peso_categoria end as peso
          from tareas
          where string::len(data.nombre_categoria) > 0 and abr = $abr
        );

        let $grades_with_peso = (
          select
            data.correo_alumno as correo_alumno,
            data.id_curso as id_curso,
            data.calificacion as calificacion,
            array::filter($tareas, |$t| $t.id_tarea = data.id_tarea)[0].categoria as categoria,
            array::filter($tareas, |$t| $t.id_tarea = data.id_tarea)[0].peso as peso
          from calificaciones
          where data.calificacion is not none
            and abr = $abr
            and array::len(array::filter($tareas, |$t| $t.id_tarea = data.id_tarea)) > 0
        );

        let $category_temp = (
          select
            correo_alumno,
            id_curso,
            categoria,
            peso,
            math::mean(calificacion) as average
          from $grades_with_peso
          where categoria is not none
          group by correo_alumno, id_curso, categoria, peso
        );

        let $category_averages = (
          select
            correo_alumno,
            id_curso,
            categoria,
            peso,
            <number>math::round(average * 100) / 100 as average
          from $category_temp
        );

        let $weighted_temp = (
          select
            correo_alumno,
            id_curso,
            math::sum(average * peso) as numerator,
            math::sum(peso) as denominator
          from $category_averages
          group by correo_alumno, id_curso
        );

        let $final_results = (
          select
            'x' as do,
            rand::guid(10) as id,
            array::filter($users, |$user| $user.correo_alumno = correo_alumno)[0].nombre_alumno as nombre_alumno,
            correo_alumno as correo_alumno,
            array::filter($users, |$user| $user.correo_alumno = correo_alumno)[0].matricula as matricula,
            id_curso as id_curso,
            array::filter($cursos, |$curso| $curso.id_curso = id_curso)[0].nombre_curso as nombre_curso,
            array::filter($cursos, |$curso| $curso.id_curso = id_curso)[0].seccion as seccion,
            array::filter($cursos, |$curso| $curso.id_curso = id_curso)[0].correo_dueno as correo_dueno,
            array::filter($cursos, |$curso| $curso.id_curso = id_curso)[0].id_servo as id_servo,
            array::filter($users, |$user| $user.correo_alumno = correo_alumno)[0].grupo as grupo,
            <number>math::round((numerator / denominator) * 100) / 100 as promedio_total
          from $weighted_temp
          order by correo_alumno
        );
        "
      .to_string(),
      Ep::Chromebooks => "
        'x' as do,
        data.deviceId as id,
        data.serialNumber as id_serie,
        data.model as modelo,
        data.annotatedUser as usuario_anotado,
        data.annotatedAssetId as id_asset,
        data.orgUnitPath as ruta_org,
        data.status as estatus,
        data.lastSync as fecha_sync,
        data.bootMode as modo_boot,
        data.lastKnownNetwork.ipAddress[0] as ip,
        data.lastKnownNetwork.wanIpAddress[0] as ipwifi,
        data.ethernetMacAddress as id_mac_ethernet,
        data.macAddress as id_mac_wifi,
        data.cpuInfo.model[0] as modelo_cpu,
        data.systemRamTotal as ram_total,
        data.diskSpaceUsage.capacityBytes as espacio_total,
        data.diskSpaceUsage.usedBytes as espacio_usado,
        data.osVersion as version_os,
        data.platformVersion as version_plataforma,
        data.deviceLicenseType as tipo_licencia,
        data.firstEnrollmentTime as fecha_enrolada,
        data.lastEnrollmentTime as fecha_reciente_enrolada,
        data.osUpdateStatus.updateTime as fecha_actualizada,
        data.autoUpdateThrough as fecha_fin_autoupdate,
        data.extendedSupportStart as fecha_extendible,
        data.extendedSupportEligible as es_extendible,
        data.extendedSupportEnabled as activa_extendible,
        data.lastDeprovisionTimestamp as fecha_desenrolada,
        data.deprovisionReason as razon_desenrolada,
        if data.recentUsers is not none then
          data.recentUsers
            .filter(|$user| $user.email is not none)
            .map(|$user| $user.email)
            .join(', ')
        else
          ''
        end as usuarios_recientes
        "
      .to_string(),
      Ep::Drives => "
        'x' as do,
        data.id as id,
        data.name as nombre,
        data.kind as tipo,      
        data.createdTime as fecha_creada,
        data.colorRgb as id_color,
        data.themeId ?? '' as id_tema,
        data.backgroundImageLink as imagen,
        data.hidden as escondida,
        if string::starts_with(data.orgUnitId, 'id:') then string::slice(data.orgUnitId, 3) else data.orgUnitId end as id_org,
        data.restrictions.adminManagedRestrictions as correo_dueno,
        data.restrictions.driveMembersOnly as solo_miembros,
        data.restrictions.domainUsersOnly as solo_dominio,
        data.restrictions.sharingFoldersRequiresOrganizerPermission as permiso_compartir,
        data.restrictions.downloadRestriction.restrictedForReaders as descargar_lector,
        data.restrictions.downloadRestriction.restrictedForWriters as descargar_escritor
        "
      .to_string(),

      // string::slice(data.orgUnitId, 3) as id_org
    
      Ep::Files => "
        'x' as do,
        data.id as id,
        data.driveId as id_drive,
        data.kind as tipo,
        data.name as nombre,
        data.mimeType as mimetype,
        data.description as descripcion,
        data.shared as compartido,
        data.trashed as en_basura,
        data.webViewLink as enlace_web,
        data.exportLinks.`application/pdf` as enlace_pdf,
        data.owners[0].emailAddress as correo_dueno,
        data.createdTime as fecha_creada,
        data.modifiedTime as fecha_modificada,
        data.lastModifyingUser.emailAddress as ultimo_modificador,
        <number>math::round((<number>data.quotaBytesUsed / 1048576) * 100) / 100 as espacio_mb_usado,
        data.writersCanShare as editores_comparten,
        data.parents[0] ?? null as id_carpeta,
        data.parents[0] ?? 'Mi unidad' as nombre_carpeta,
        array::join(data.permissions.*.emailAddress ?? [] ,', ') as correos_compartidos,
        array::join(data.permissions.*.role ?? [], ', ') as permisos_compartidos
        "
      .to_string(),
    // FIX the email owner has to come from value
      Ep::Calendars => "
        'x' as do,
        data.id as id,
        data.summary as nombre,
        data.description as descripcion,
        extra.key1 as correo_dueno,
        data.accessRole as rol_acceso,
        data.location as ubicacion,
        data.timeZone as zona_horaria,
        data.colorId as id_color,
        data.foregroundColor as color_principal,
        data.backgroundColor as color_secundario,
        data.primary ?? false as principal,
        data.selected ?? false as elegido,
        data.hidden ?? false as escondido,
        data.deleted ?? false as eliminado
        "
      .to_string(),
    // FIX the email owner, id_calendario has to come from value
      Ep::Events => "
        'x' as do,
        data.id as id,
        data.summary as nombre_evento,
        data.description ?? '' as descripcion,
        extra.key1 as id_calendario,
        (select value data.nombre from calendarios
          where data.id = $parent.extra.key1
          and abr = $abr
        )[0] as nombre_calendario,
        (select value data.correo_dueno from calendarios
          where data.id = $parent.extra.key1
          and abr = $abr
        )[0] as correo_dueno,
        data.creator.email as correo_creador,
        data.organizer.email as correo_organizador,
        data.created as fecha_creada,
        data.updated as fecha_actualizada,
        if data.start.dateTime is not none then
          data.start.dateTime
        else if data.start.date is not none then
          data.start.date
        else
          ''
        end as fecha_inicia,
        if data.end.dateTime is not none then
          data.end.dateTime
        else if data.end.date is not none then
          data.end.date
        else
          ''
        end as fecha_termina,
        data.hangoutLink ?? '' as enlace_meet,
        array::join(data.attendees.*.email ?? [], ', ') as invitados,
        data.kind as tipo,
        data.eventType ?? 'default' as tipo_evento,
        data.status as estatus,
        data.sendUpdates as notificaciones,
        data.htmlLink as enlace_web,
        data.location ?? '' as ubicacion,
        data.colorId ?? '' as id_color,
        data.transparency ?? 'opaque' as transparencia,
        data.visibility ?? 'default' as visibilidad,
        data.guestsCanInviteOthers ?? true as pueden_invitar,
        data.guestsCanModify ?? false as pueden_modificar,
        data.guestsCanSeeOtherGuests ?? true as pueden_verles,
        data.anyoneCanAddSelf ?? false as pueden_agregarse,
        if data.attachments is not none then
          data.attachments
            .filter(|$material| $material.fileUrl is not none)
            .map(|$material| $material.fileUrl)
            .join(', ')
        else
          ''
        end as anexos,
        if data.recurrence is not none then
          array::join(data.recurrence, ', ')
        else
          ''
        end as recurrencia
        "
      .to_string(),
      Ep::Spaces => "
        'x' as do,
        data.name as id,
        data.displayName as nombre,
        data.spaceDetails.description ?? '' as descripcion,
        data.createTime as fecha_creada,
        data.lastActiveTime as fecha_actualizada,
        data.externalUserAllowed ?? false as permite_externos,
        data.spaceHistoryState as historial_activo,        
        data.threaded ?? false as threads,
        data.adminInstalled ?? false as admin_instalado,
        data.type as tipo,
        data.spaceType as tipo_espacio,
        data.singleUserBotDm ?? false as es_bot,
        data.spaceUri as url,
        data.membershipCount.joinedDirectHumanUserCount as cuenta_miembros
        "
      .to_string(),
      Ep::UsageReports => "
        'x' as do,
        rand::guid(10) as id,
        data.entity.userEmail as correo_usuario,
        data.date as fecha,
        (
          if array::len(array::filter(data.parameters, |$param| $param.name == 'accounts:is_suspended')) > 0 then 
            array::filter(data.parameters, |$param| $param.name == 'accounts:is_suspended')[0].boolValue 
          else 
            false 
          end
        ) as suspendido,
        (
          if array::len(array::filter(data.parameters, |$param| $param.name == 'accounts:is_super_admin')) > 0 then 
            array::filter(data.parameters, |$param| $param.name == 'accounts:is_super_admin')[0].boolValue 
          else 
            false 
          end
        ) as es_superadmin,
        (
          if array::len(array::filter(data.parameters, |$param| $param.name == 'accounts:is_delegated_admin')) > 0 then 
            array::filter(data.parameters, |$param| $param.name == 'accounts:is_delegated_admin')[0].boolValue 
          else 
            false 
          end
        ) as es_admin_delegado,
        (
          if array::len(array::filter(data.parameters, |$param| $param.name == 'accounts:is_2sv_enrolled')) > 0 then 
            array::filter(data.parameters, |$param| $param.name == 'accounts:is_2sv_enrolled')[0].boolValue 
          else 
            false 
          end
        ) as tiene_2pasos,
        (
          if array::len(array::filter(data.parameters, |$param| $param.name == 'accounts:creation_time')) > 0 then 
            string::replace(string::replace(array::filter(data.parameters, |$param| $param.name == 'accounts:creation_time')[0].datetimeValue, 'T', ' '), 'Z', '') 
          else 
            '' 
          end
        ) as fecha_creada,
        (
          if array::len(array::filter(data.parameters, |$param| $param.name == 'accounts:last_login_time')) > 0 then 
            string::replace(string::replace(array::filter(data.parameters, |$param| $param.name == 'accounts:last_login_time')[0].datetimeValue, 'T', ' '), 'Z', '') 
          else 
            '' 
          end
        ) as login_ultimo,
        (
          if array::len(array::filter(data.parameters, |$param| $param.name == 'gmail:last_access_time')) > 0 then 
            string::replace(string::replace(array::filter(data.parameters, |$param| $param.name == 'gmail:last_access_time')[0].datetimeValue, 'T', ' '), 'Z', '') 
          else 
            '' 
          end
        ) as ultimo_acceso_gmail,
        (
          if array::len(array::filter(data.parameters, |$param| $param.name == 'accounts:used_quota_in_mb')) > 0 then 
            <int>array::filter(data.parameters, |$param| $param.name == 'accounts:used_quota_in_mb')[0].intValue 
          else 
            0 
          end
        ) as total_mb,
        (
          <int>array::filter(data.parameters, |$param| $param.name == 'accounts:drive_used_quota_in_mb')[0].intValue
        ) ?? 0 as drive_mb,
        (
          <int>array::filter(data.parameters, |$param| $param.name == 'accounts:gmail_used_quota_in_mb')[0].intValue
        ) ?? 0 as gmail_mb,
        (
          <int>array::filter(data.parameters, |$param| $param.name == 'accounts:gplus_photos_used_quota_in_mb')[0].intValue
        ) ?? 0 as photos_mb,
        (
          <int>array::filter(data.parameters, |$param| $param.name == 'gmail:num_emails_exchanged')[0].intValue
        ) ?? 0 as correos_exch,
        (
          <int>array::filter(data.parameters, |$param| $param.name == 'drive:num_items_created')[0].intValue
        ) ?? 0 as drive_creados,
        (
          <int>array::filter(data.parameters, |$param| $param.name == 'drive:num_google_documents_created')[0].intValue
        ) ?? 0 as docs_creados,
        (
          <int>array::filter(data.parameters, |$param| $param.name == 'drive:num_google_spreadsheets_created')[0].intValue
        ) ?? 0 as sheets_creados,
        (
          <int>array::filter(data.parameters, |$param| $param.name == 'drive:num_google_presentations_created')[0].intValue
        ) ?? 0 as slides_creados,
        (
          <int>array::filter(data.parameters, |$param| $param.name == 'drive:num_google_forms_created')[0].intValue
        ) ?? 0 as forms_creados,
        (
          <int>array::filter(data.parameters, |$param| $param.name == 'drive:num_google_sites_created')[0].intValue
        ) ?? 0 as sites_creados,
        (
          <int>array::filter(data.parameters, |$param| $param.name == 'classroom:num_courses_created')[0].intValue
        ) ?? 0 as cursos_creados,
        (
          <int>array::filter(data.parameters, |$param| $param.name == 'classroom:num_posts_created')[0].intValue
        ) ?? 0 as posts_classroom,
        (
          if array::len(array::filter(data.parameters, |$param| $param.name == 'classroom:role')) > 0 then 
            array::filter(data.parameters, |$param| $param.name == 'classroom:role')[0].stringValue 
          else 
            '' 
          end
        ) as rol_classroom
        "
      .to_string(),
    };

    let query_template_str = match self {
      Ep::Gradsav | Ep::Gradspon => {
        r#"
          {}
          
          for $record in $final_results { 
            create type::thing($table, rand::uuid::v7())
            set data = $record, abr = $abr, ers = '';
          };
        "#
      }
      _ => {
        r#"
          select
            {} 
          from type::table($gtable) 
          where abr = $abr
        "#
      }
    };

    query_template_str.replace("{}", &item)
  }
}

pub async fn durs(dur: usize) -> String {
  if dur <= 1000 {
    format!("{} segundo", 1)
  } else if dur > 1000 && dur < 60000 {
    format!("{} segundos", dur / 1000)
  } else if (60000..120000).contains(&dur) {
    format!("un poco más de {} minuto", dur / 1000 / 60)
  } else {
    format!("{} minutos", dur / 1000 / 60)
  }
}

pub fn lcs() -> Vec<Value> {
  vec![
    json!({
      "nombre_producto": "Google Workspace for Education",
      "pid": "101047",
      "nombre_sku": "Gemini Education",
      "skuid": "1010470004",
      "abrlic": "EGEMW-N"
    }),
    json!({
      "nombre_producto": "Google Workspace for Education",
      "pid": "101047",
      "nombre_sku": "Gemini Education Premium",
      "skuid": "1010470005",
      "abrlic": "EGEMW-P"
    }),
    // json!({
    //   "nombre_producto": "Google Workspace for Education",
    //   "pid": "Google-Apps",
    //   "nombre_sku": "Google Workspace for Education - Fundamentals",
    //   "skuid": "Google-Apps-For-Education",
    //   "abrlic": "EFUNW-L"
    // }),
    json!({
      "nombre_producto": "Google Workspace for Education",
      "pid": "Google-Apps",
      "nombre_sku": "Google Workspace for Education Fundamentals",
      "skuid": "1010070001",
      "abrlic": "EFUNW"
    }),
    json!({
      "nombre_producto": "Google Workspace for Education",
      "pid": "Google-Apps",
      "nombre_sku": "Google Workspace for Education Fundamentals - Archived User",
      "skuid": "1010340007",
      "abrlic": "EFUNW-A"
    }),
    json!({
      "nombre_producto": "Google Workspace for Education",
      "pid": "Google-Apps",
      "nombre_sku": "Google Workspace for Education Gmail Only User",
      "skuid": "1010070004",
      "abrlic": "EFUNW-G"
    }),
    json!({
      "nombre_producto": "Google Workspace for Education",
      "pid": "101031",
      "nombre_sku": "Google Workspace for Education Plus",
      "skuid": "1010310008",
      "abrlic": "EPLUSW-S"
    }),
    json!({
      "nombre_producto": "Google Workspace for Education",
      "pid": "101031",
      "nombre_sku": "Google Workspace for Education Plus (Staff)",
      "skuid": "1010310009",
      "abrlic": "EPLUSW-T"
    }),
    json!({
      "nombre_producto": "Google Workspace for Education",
      "pid": "101037",
      "nombre_sku": "Google Workspace for Education: Teaching and Learning Upgrade",
      "skuid": "1010370001",
      "abrlic": "ETEACHW-T"
    }),
    json!({
      "nombre_producto": "Google Workspace for Education",
      "pid": "101031",
      "nombre_sku": "Google Workspace for Education Plus - Legacy (Student)",
      "skuid": "1010310003",
      "abrlic": "EPLUSL-S"
    }),
    json!({
      "nombre_producto": "Google Workspace for Education",
      "pid": "101031",
      "nombre_sku": "Google Workspace for Education Plus - Legacy",
      "skuid": "1010310002",
      "abrlic": "EPLUSL-T"
    }),
    json!({
      "nombre_producto": "Google Workspace for Education",
      "pid": "101031",
      "nombre_sku": "Google Workspace for Education Standard",
      "skuid": "1010310005",
      "abrlic": "ESTANW-S"
    }),
    json!({
      "nombre_producto": "Google Workspace for Education",
      "pid": "101031",
      "nombre_sku": "Google Workspace for Education Standard (Staff)",
      "skuid": "1010310006",
      "abrlic": "ESTANW-T"
    }),
    json!({
      "nombre_producto": "Google Workspace for Education",
      "pid": "101031",
      "nombre_sku": "Google Workspace for Education Standard (Extra Student)",
      "skuid": "1010310007",
      "abrlic": "ESTANW-EX"
    }),
    json!({
      "nombre_producto": "Google Workspace for Education",
      "pid": "101031",
      "nombre_sku": "Google Workspace for Education Plus (Extra Student)",
      "skuid": "1010310010",
      "abrlic": "EPLUSW-EX"
    }),
    json!({
      "nombre_producto": "Google Workspace",
      "pid": "Google-Apps",
      "nombre_sku": "Google Workspace Business Starter",
      "skuid": "1010020027",
      "abrlic": "BSTAR"
    }),
    json!({
      "nombre_producto": "Google Workspace",
      "pid": "Google-Apps",
      "nombre_sku": "Google Workspace Business Standard",
      "skuid": "1010020028",
      "abrlic": "BSTAN"
    }),
    json!({
      "nombre_producto": "Google Workspace",
      "pid": "Google-Apps",
      "nombre_sku": "Google Workspace Business Plus",
      "skuid": "1010020025",
      "abrlic": "BPLUS"
    }),
    json!({
      "nombre_producto": "Google Workspace",
      "pid": "Google-Apps",
      "nombre_sku": "Google Workspace Enterprise Standard",
      "skuid": "1010020026",
      "abrlic": "BSTAN-E"
    }),
    json!({
      "nombre_producto": "Google Workspace",
      "pid": "Google-Apps",
      "nombre_sku": "Google Workspace Enterprise Plus",
      "skuid": "1010020020",
      "abrlic": "BPLUS-E"
    }),
    json!({
      "nombre_producto": "Google Workspace",
      "pid": "Google-Apps",
      "nombre_sku": "Google Workspace Frontline",
      "skuid": "1010020030",
      "abrlic": "BFRONT"
    }),
    json!({
      "nombre_producto": "Google Workspace",
      "pid": "Google-Apps",
      "nombre_sku": "Google Workspace Enterprise Essentials",
      "skuid": "1010060003",
      "abrlic": "BESEN-E"
    }),
  ]
}
