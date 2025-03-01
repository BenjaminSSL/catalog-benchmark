package unity

type CreateCatalogBody struct {
	Name       string      `json:"name"`
	Comment    string      `json:"comment,omitempty"`
	Properties interface{} `json:"properties,omitempty"`
}

type UpdateCatalogBody struct {
	Comment    string      `json:"comment,omitempty"`
	Properties interface{} `json:"properties,omitempty"`
	NewName    string      `json:"new_name,omitempty"`
}
