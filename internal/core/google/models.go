package google

type User struct {
	Email string
	Roles map[string]bool
}

type Model struct {
	Users []*User
}

func (m *Model) LookupUser(email string) *User {
	for _,p := range m.Users {
		if p.Email == email {
			return p
		}
	}
	return nil
}

func (m *Model) DeclareUser(email string) *User {
	existing := m.LookupUser(email)
	if existing != nil {
		return existing
	}

	newUser := &User { Email:email, Roles: make(map[string]bool)  }
	m.Users = append(m.Users, newUser)
	return newUser
}

func (u *User) Assign(role string) {
	u.Roles[role] = true
}

func (g *User) AssignedTo() []string {
	roles := make([]string, 0, len(g.Roles))
	for k := range g.Roles {
		roles = append(roles, k)
	}
	return roles
}

